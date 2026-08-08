"""Pure contract for the one-shot quality-assessment legacy import.

The import is intentionally modelled independently from any ORM or migration
runner.  Core owns the immutable candidate plan, strict legacy selectors,
checkpoint semantics, and completion postcondition.  Community adapters own
the physical ledger and must implement the transaction guarantees documented
by the corresponding port.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from okto_pulse.core.domain.quality_assessment import (
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentScale,
    AssessmentScaleKind,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentVersionSet,
    ScoreDirection,
)
from okto_pulse.core.domain.quality_canonicalization import (
    canonical_json_bytes,
    canonical_sha256,
)

QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH = (
    "quality-assessment-legacy-import/v1"
)
QUALITY_ASSESSMENT_LEGACY_IMPORT_CONTRACT_VERSION = (
    "quality-assessment-legacy-import-contract/v1"
)

# This manifest is deliberately data, not a hash of Python bytecode.  Any
# selector-policy change requires a new manifest version and therefore a new
# code digest (and normally a new epoch).
QUALITY_ASSESSMENT_LEGACY_SELECTOR_MANIFEST_V1: dict[str, Any] = {
    "epoch": QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH,
    "ideation": {
        "source": "scope_assessment.ambiguity",
        "scale": [1, 5],
        "gate": "enabled_at_initial_cutoff_snapshot",
        "coercion": "integer_or_integral_float_or_integer_string",
        "bool": "invalid",
    },
    "spec": {
        "source": "validations[current_validation_id].ambiguity",
        "scale": [0, 100],
        "required_outcome": "success",
        "required_scope": "same_board_and_spec",
        "coercion": "finite_number",
        "bool": "invalid",
    },
    "excluded_subjects": ["refinement"],
    "excluded_payloads": ["findings", "manual_checklist_ref"],
    "native_assessment": "wins",
}
QUALITY_ASSESSMENT_LEGACY_IMPORT_CODE_DIGEST = canonical_sha256(
    QUALITY_ASSESSMENT_LEGACY_SELECTOR_MANIFEST_V1
)


class LegacyImportContractError(ValueError):
    """A value violates the frozen legacy-import contract."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise LegacyImportContractError(code)
    normalized = value.strip()
    if not normalized:
        raise LegacyImportContractError(code)
    return normalized


def _strict_non_negative_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise LegacyImportContractError(code)
    return value


def _strict_positive_int(value: object, code: str) -> int:
    resolved = _strict_non_negative_int(value, code)
    if resolved < 1:
        raise LegacyImportContractError(code)
    return resolved


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise LegacyImportContractError(code)
    return value.astimezone(timezone.utc)


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if len(normalized) != 64:
        raise LegacyImportContractError(code)
    try:
        int(normalized, 16)
    except ValueError as exc:
        raise LegacyImportContractError(code) from exc
    return normalized


def _capture_json(value: object, code: str) -> str:
    try:
        return canonical_json_bytes(value).decode("utf-8")
    except (TypeError, ValueError) as exc:
        raise LegacyImportContractError(code) from exc


def _load_json(value: str, code: str) -> object:
    try:
        return json.loads(value)
    except (TypeError, ValueError) as exc:
        raise LegacyImportContractError(code) from exc


class LegacyImportSelectionReason(str, Enum):
    ELIGIBLE = "eligible"
    AFTER_CUTOFF = "after_cutoff"
    SUBJECT_INELIGIBLE = "subject_ineligible"
    IDEATION_GATE_DISABLED = "ideation_gate_disabled"
    LEGACY_PAYLOAD_INVALID = "legacy_payload_invalid"
    SCORE_MISSING = "score_missing"
    SCORE_INVALID = "score_invalid"
    SCORE_OUT_OF_RANGE = "score_out_of_range"
    CURRENT_VALIDATION_MISSING = "current_validation_missing"
    CURRENT_VALIDATION_NOT_FOUND = "current_validation_not_found"
    CURRENT_VALIDATION_DUPLICATE = "current_validation_duplicate"
    VALIDATION_SCOPE_MISMATCH = "validation_scope_mismatch"
    VALIDATION_NOT_SUCCESSFUL = "validation_not_successful"


class LegacyImportResolution(str, Enum):
    IMPORTED = "imported"
    NATIVE_WINS = "native_wins"


@dataclass(frozen=True, slots=True)
class LegacyIdeationSnapshot:
    """Immutable projection of the legacy Ideation source at ``cutoff``."""

    subject: AssessmentSubjectRef
    observed_at: datetime
    active_at_cutoff: bool
    gate_enabled_at_cutoff: bool
    scope_assessment_json: str
    digests: AssessmentDigestSet
    versions: AssessmentVersionSet

    def __post_init__(self) -> None:
        if (
            not isinstance(self.subject, AssessmentSubjectRef)
            or self.subject.subject_type is not AssessmentSubjectType.IDEATION
        ):
            raise LegacyImportContractError(
                "legacy_ideation_subject_invalid"
            )
        object.__setattr__(
            self,
            "observed_at",
            _aware_utc(
                self.observed_at,
                "legacy_ideation_observed_at_must_be_aware",
            ),
        )
        if not isinstance(self.active_at_cutoff, bool):
            raise LegacyImportContractError(
                "legacy_ideation_active_flag_invalid"
            )
        if not isinstance(self.gate_enabled_at_cutoff, bool):
            raise LegacyImportContractError(
                "legacy_ideation_gate_flag_invalid"
            )
        payload = _load_json(
            self.scope_assessment_json,
            "legacy_ideation_scope_assessment_json_invalid",
        )
        canonical = _capture_json(
            payload,
            "legacy_ideation_scope_assessment_json_invalid",
        )
        if canonical != self.scope_assessment_json:
            raise LegacyImportContractError(
                "legacy_ideation_scope_assessment_not_canonical"
            )
        if not isinstance(self.digests, AssessmentDigestSet):
            raise LegacyImportContractError("legacy_ideation_digests_invalid")
        if not isinstance(self.versions, AssessmentVersionSet):
            raise LegacyImportContractError("legacy_ideation_versions_invalid")

    @classmethod
    def capture(
        cls,
        *,
        subject: AssessmentSubjectRef,
        observed_at: datetime,
        active_at_cutoff: bool,
        gate_enabled_at_cutoff: bool,
        scope_assessment: object,
        digests: AssessmentDigestSet,
        versions: AssessmentVersionSet,
    ) -> "LegacyIdeationSnapshot":
        return cls(
            subject=subject,
            observed_at=observed_at,
            active_at_cutoff=active_at_cutoff,
            gate_enabled_at_cutoff=gate_enabled_at_cutoff,
            scope_assessment_json=_capture_json(
                scope_assessment,
                "legacy_ideation_scope_assessment_invalid",
            ),
            digests=digests,
            versions=versions,
        )

    @property
    def scope_assessment(self) -> object:
        return _load_json(
            self.scope_assessment_json,
            "legacy_ideation_scope_assessment_json_invalid",
        )


@dataclass(frozen=True, slots=True)
class LegacySpecSnapshot:
    """Immutable projection of a Spec and its append-only validations."""

    subject: AssessmentSubjectRef
    observed_at: datetime
    active_at_cutoff: bool
    current_validation_id: str | None
    validations_json: str
    digests: AssessmentDigestSet
    versions: AssessmentVersionSet

    def __post_init__(self) -> None:
        if (
            not isinstance(self.subject, AssessmentSubjectRef)
            or self.subject.subject_type is not AssessmentSubjectType.SPEC
        ):
            raise LegacyImportContractError("legacy_spec_subject_invalid")
        object.__setattr__(
            self,
            "observed_at",
            _aware_utc(
                self.observed_at,
                "legacy_spec_observed_at_must_be_aware",
            ),
        )
        if not isinstance(self.active_at_cutoff, bool):
            raise LegacyImportContractError(
                "legacy_spec_active_flag_invalid"
            )
        if self.current_validation_id is not None:
            object.__setattr__(
                self,
                "current_validation_id",
                _required_text(
                    self.current_validation_id,
                    "legacy_current_validation_id_invalid",
                ),
            )
        payload = _load_json(
            self.validations_json,
            "legacy_spec_validations_json_invalid",
        )
        canonical = _capture_json(
            payload,
            "legacy_spec_validations_json_invalid",
        )
        if canonical != self.validations_json:
            raise LegacyImportContractError(
                "legacy_spec_validations_not_canonical"
            )
        if not isinstance(self.digests, AssessmentDigestSet):
            raise LegacyImportContractError("legacy_spec_digests_invalid")
        if not isinstance(self.versions, AssessmentVersionSet):
            raise LegacyImportContractError("legacy_spec_versions_invalid")

    @classmethod
    def capture(
        cls,
        *,
        subject: AssessmentSubjectRef,
        observed_at: datetime,
        active_at_cutoff: bool,
        current_validation_id: str | None,
        validations: object,
        digests: AssessmentDigestSet,
        versions: AssessmentVersionSet,
    ) -> "LegacySpecSnapshot":
        return cls(
            subject=subject,
            observed_at=observed_at,
            active_at_cutoff=active_at_cutoff,
            current_validation_id=current_validation_id,
            validations_json=_capture_json(
                validations,
                "legacy_spec_validations_invalid",
            ),
            digests=digests,
            versions=versions,
        )

    @property
    def validations(self) -> object:
        return _load_json(
            self.validations_json,
            "legacy_spec_validations_json_invalid",
        )


@dataclass(frozen=True, slots=True)
class LegacyImportSourceSnapshot:
    """All source rows observed through one adapter-owned cutoff snapshot."""

    board_id: str
    cutoff: datetime
    ideations: tuple[LegacyIdeationSnapshot, ...] = ()
    specs: tuple[LegacySpecSnapshot, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "legacy_import_source_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "cutoff",
            _aware_utc(
                self.cutoff,
                "legacy_import_cutoff_must_be_aware",
            ),
        )
        object.__setattr__(self, "ideations", tuple(self.ideations))
        object.__setattr__(self, "specs", tuple(self.specs))
        if any(
            not isinstance(item, LegacyIdeationSnapshot)
            for item in self.ideations
        ):
            raise LegacyImportContractError(
                "legacy_import_ideation_snapshot_invalid"
            )
        if any(not isinstance(item, LegacySpecSnapshot) for item in self.specs):
            raise LegacyImportContractError(
                "legacy_import_spec_snapshot_invalid"
            )
        if any(
            item.subject.board_id != self.board_id
            for item in (*self.ideations, *self.specs)
        ):
            raise LegacyImportContractError(
                "legacy_import_source_board_scope_mismatch"
            )


@dataclass(frozen=True, slots=True)
class LegacyImportCandidateKey:
    """The five-column physical uniqueness key required by BR15."""

    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    assessment_kind: AssessmentKind
    epoch: str = QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "legacy_import_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(
                self.subject_id,
                "legacy_import_subject_id_required",
            ),
        )
        if self.subject_type not in {
            AssessmentSubjectType.IDEATION,
            AssessmentSubjectType.SPEC,
        }:
            raise LegacyImportContractError(
                "legacy_import_subject_type_unsupported"
            )
        if not isinstance(self.assessment_kind, AssessmentKind):
            raise LegacyImportContractError(
                "legacy_import_assessment_kind_invalid"
            )
        if self.epoch != QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH:
            raise LegacyImportContractError(
                "legacy_import_epoch_unsupported"
            )
        expected_kind = (
            AssessmentKind.AMBIGUITY
            if self.subject_type is AssessmentSubjectType.IDEATION
            else AssessmentKind.SPEC_VALIDATION
        )
        if self.assessment_kind is not expected_kind:
            raise LegacyImportContractError(
                "legacy_import_subject_kind_mismatch"
            )

    @property
    def physical_identity(self) -> tuple[str, str, str, str, str]:
        return (
            self.board_id,
            self.subject_type.value,
            self.subject_id,
            self.assessment_kind.value,
            self.epoch,
        )


@dataclass(frozen=True, slots=True)
class LegacyImportCandidate:
    key: LegacyImportCandidateKey
    subject_version: int
    scale: AssessmentScale
    score: float
    justification: str
    digests: AssessmentDigestSet
    versions: AssessmentVersionSet
    legacy_source_id: str
    legacy_source_digest: str
    observed_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.key, LegacyImportCandidateKey):
            raise LegacyImportContractError("legacy_import_key_invalid")
        object.__setattr__(
            self,
            "subject_version",
            _strict_positive_int(
                self.subject_version,
                "legacy_import_subject_version_invalid",
            ),
        )
        if not isinstance(self.scale, AssessmentScale):
            raise LegacyImportContractError("legacy_import_scale_invalid")
        object.__setattr__(self, "score", self.scale.validate_score(self.score))
        object.__setattr__(
            self,
            "justification",
            _required_text(
                self.justification,
                "legacy_import_justification_required",
            ),
        )
        if not isinstance(self.digests, AssessmentDigestSet):
            raise LegacyImportContractError("legacy_import_digests_invalid")
        if not isinstance(self.versions, AssessmentVersionSet):
            raise LegacyImportContractError("legacy_import_versions_invalid")
        object.__setattr__(
            self,
            "legacy_source_id",
            _required_text(
                self.legacy_source_id,
                "legacy_import_source_id_required",
            ),
        )
        object.__setattr__(
            self,
            "legacy_source_digest",
            _sha256(
                self.legacy_source_digest,
                "legacy_import_source_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "observed_at",
            _aware_utc(
                self.observed_at,
                "legacy_import_observed_at_must_be_aware",
            ),
        )

    def canonical_payload(self) -> dict[str, object]:
        return {
            "key": self.key.physical_identity,
            "subject_version": self.subject_version,
            "scale": {
                "kind": self.scale.kind.value,
                "minimum": self.scale.minimum,
                "maximum": self.scale.maximum,
                "direction": self.scale.direction.value,
            },
            "score": self.score,
            "justification": self.justification,
            "digests": {
                "content": self.digests.content_digest,
                "clarification": self.digests.clarification_digest,
                "ruleset": self.digests.ruleset_digest,
                "taxonomy": self.digests.taxonomy_digest,
                "policy": self.digests.policy_digest,
                "input": self.digests.input_digest,
                "canonicalization": self.digests.canonicalization_version,
            },
            "versions": {
                "ruleset": self.versions.ruleset_version,
                "taxonomy": self.versions.taxonomy_version,
                "analyzer": self.versions.analyzer_version,
                "policy": self.versions.policy_version,
            },
            "legacy_source_id": self.legacy_source_id,
            "legacy_source_digest": self.legacy_source_digest,
            "observed_at": self.observed_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class LegacyImportSelection:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    reason: LegacyImportSelectionReason
    candidate: LegacyImportCandidate | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "legacy_selection_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(
                self.subject_id,
                "legacy_selection_subject_id_required",
            ),
        )
        if self.subject_type not in {
            AssessmentSubjectType.IDEATION,
            AssessmentSubjectType.SPEC,
        }:
            raise LegacyImportContractError(
                "legacy_selection_subject_type_invalid"
            )
        if not isinstance(self.reason, LegacyImportSelectionReason):
            raise LegacyImportContractError(
                "legacy_selection_reason_invalid"
            )
        if (self.reason is LegacyImportSelectionReason.ELIGIBLE) != (
            self.candidate is not None
        ):
            raise LegacyImportContractError(
                "legacy_selection_candidate_mismatch"
            )
        if self.candidate is not None:
            if (
                self.candidate.key.board_id != self.board_id
                or self.candidate.key.subject_type is not self.subject_type
                or self.candidate.key.subject_id != self.subject_id
            ):
                raise LegacyImportContractError(
                    "legacy_selection_candidate_scope_mismatch"
                )


@dataclass(frozen=True, slots=True)
class LegacyImportRejectionCount:
    reason: LegacyImportSelectionReason
    count: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.reason, LegacyImportSelectionReason)
            or self.reason is LegacyImportSelectionReason.ELIGIBLE
        ):
            raise LegacyImportContractError(
                "legacy_rejection_reason_invalid"
            )
        object.__setattr__(
            self,
            "count",
            _strict_positive_int(
                self.count,
                "legacy_rejection_count_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class LegacyImportPlan:
    """Immutable, canonically ordered candidate plan."""

    board_id: str
    cutoff: datetime
    code_digest: str
    candidates: tuple[LegacyImportCandidate, ...]
    rejection_counts: tuple[LegacyImportRejectionCount, ...] = ()
    candidate_digest: str | None = None
    epoch: str = QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH
    contract_version: str = QUALITY_ASSESSMENT_LEGACY_IMPORT_CONTRACT_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "legacy_import_plan_board_id_required",
            ),
        )
        if self.epoch != QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH:
            raise LegacyImportContractError(
                "legacy_import_epoch_unsupported"
            )
        if (
            self.contract_version
            != QUALITY_ASSESSMENT_LEGACY_IMPORT_CONTRACT_VERSION
        ):
            raise LegacyImportContractError(
                "legacy_import_contract_version_unsupported"
            )
        object.__setattr__(
            self,
            "cutoff",
            _aware_utc(
                self.cutoff,
                "legacy_import_cutoff_must_be_aware",
            ),
        )
        object.__setattr__(
            self,
            "code_digest",
            _sha256(
                self.code_digest,
                "legacy_import_code_digest_invalid",
            ),
        )
        candidates = tuple(self.candidates)
        rejection_counts = tuple(self.rejection_counts)
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "rejection_counts", rejection_counts)
        if any(
            not isinstance(candidate, LegacyImportCandidate)
            for candidate in candidates
        ):
            raise LegacyImportContractError(
                "legacy_import_candidate_invalid"
            )
        if any(
            candidate.key.board_id != self.board_id
            for candidate in candidates
        ):
            raise LegacyImportContractError(
                "legacy_import_candidate_board_scope_mismatch"
            )
        expected_order = tuple(
            sorted(candidates, key=lambda item: item.key.physical_identity)
        )
        if candidates != expected_order:
            raise LegacyImportContractError(
                "legacy_import_candidate_order_invalid"
            )
        identities = [candidate.key.physical_identity for candidate in candidates]
        if len(set(identities)) != len(identities):
            raise LegacyImportContractError(
                "legacy_import_candidate_identity_duplicate"
            )
        if any(candidate.observed_at > self.cutoff for candidate in candidates):
            raise LegacyImportContractError(
                "legacy_import_candidate_after_cutoff"
            )
        if any(
            not isinstance(item, LegacyImportRejectionCount)
            for item in rejection_counts
        ):
            raise LegacyImportContractError(
                "legacy_import_rejection_count_invalid"
            )
        reasons = [item.reason for item in rejection_counts]
        if len(set(reasons)) != len(reasons) or reasons != sorted(
            reasons,
            key=lambda item: item.value,
        ):
            raise LegacyImportContractError(
                "legacy_import_rejection_order_invalid"
            )
        computed_digest = canonical_sha256(
            [candidate.canonical_payload() for candidate in candidates]
        )
        if self.candidate_digest is None:
            object.__setattr__(self, "candidate_digest", computed_digest)
        elif (
            _sha256(
                self.candidate_digest,
                "legacy_import_candidate_digest_invalid",
            )
            != computed_digest
        ):
            raise LegacyImportContractError(
                "legacy_import_candidate_digest_mismatch"
            )

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def rejected_count(self) -> int:
        return sum(item.count for item in self.rejection_counts)

    @property
    def scanned_count(self) -> int:
        return self.candidate_count + self.rejected_count

    @property
    def plan_digest(self) -> str:
        return canonical_sha256(
            {
                "board_id": self.board_id,
                "epoch": self.epoch,
                "contract_version": self.contract_version,
                "cutoff": self.cutoff.isoformat(),
                "code_digest": self.code_digest,
                "candidate_digest": self.candidate_digest,
                "candidate_count": self.candidate_count,
                "rejections": [
                    {"reason": item.reason.value, "count": item.count}
                    for item in self.rejection_counts
                ],
            }
        )


@dataclass(frozen=True, slots=True)
class LegacyImportCheckpoint:
    board_id: str
    epoch: str
    plan_digest: str
    candidate_digest: str
    processed_count: int
    imported_count: int
    native_wins_count: int
    cursor_ordinal: int | None
    last_candidate_key: LegacyImportCandidateKey | None
    updated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "legacy_checkpoint_board_id_required",
            ),
        )
        if self.epoch != QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH:
            raise LegacyImportContractError(
                "legacy_checkpoint_epoch_invalid"
            )
        object.__setattr__(
            self,
            "plan_digest",
            _sha256(
                self.plan_digest,
                "legacy_checkpoint_plan_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "candidate_digest",
            _sha256(
                self.candidate_digest,
                "legacy_checkpoint_candidate_digest_invalid",
            ),
        )
        for field_name in (
            "processed_count",
            "imported_count",
            "native_wins_count",
        ):
            object.__setattr__(
                self,
                field_name,
                _strict_non_negative_int(
                    getattr(self, field_name),
                    f"legacy_checkpoint_{field_name}_invalid",
                ),
            )
        if (
            self.imported_count + self.native_wins_count
            != self.processed_count
        ):
            raise LegacyImportContractError(
                "legacy_checkpoint_count_mismatch"
            )
        if self.processed_count == 0:
            if self.cursor_ordinal is not None or self.last_candidate_key is not None:
                raise LegacyImportContractError(
                    "legacy_checkpoint_initial_cursor_invalid"
                )
        else:
            if (
                self.cursor_ordinal != self.processed_count - 1
                or not isinstance(
                    self.last_candidate_key,
                    LegacyImportCandidateKey,
                )
            ):
                raise LegacyImportContractError(
                    "legacy_checkpoint_cursor_invalid"
                )
            if self.last_candidate_key.board_id != self.board_id:
                raise LegacyImportContractError(
                    "legacy_checkpoint_board_scope_mismatch"
                )
        object.__setattr__(
            self,
            "updated_at",
            _aware_utc(
                self.updated_at,
                "legacy_checkpoint_updated_at_must_be_aware",
            ),
        )

    @classmethod
    def initial(
        cls,
        *,
        plan: LegacyImportPlan,
        updated_at: datetime,
    ) -> "LegacyImportCheckpoint":
        return cls(
            board_id=plan.board_id,
            epoch=plan.epoch,
            plan_digest=plan.plan_digest,
            candidate_digest=plan.candidate_digest or "",
            processed_count=0,
            imported_count=0,
            native_wins_count=0,
            cursor_ordinal=None,
            last_candidate_key=None,
            updated_at=updated_at,
        )


@dataclass(frozen=True, slots=True)
class LegacyImportWriteIdentity:
    """Deterministic receipt and audit identity owned by Core policy."""

    receipt_id: str
    event_id: str
    history_id: str
    outbox_id: str
    idempotency_key: str
    request_digest: str
    run_identity_digest: str
    authority_digest: str
    actor_id: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "receipt_id",
            "event_id",
            "history_id",
            "outbox_id",
            "idempotency_key",
            "actor_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"legacy_write_{field_name}_required",
                ),
            )
        for field_name in (
            "request_digest",
            "run_identity_digest",
            "authority_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"legacy_write_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "legacy_write_occurred_at_must_be_aware",
            ),
        )


@dataclass(frozen=True, slots=True)
class LegacyImportCandidateWork:
    """One adapter operation: resolve candidate and advance checkpoint."""

    plan_digest: str
    candidate_digest: str
    ordinal: int
    candidate: LegacyImportCandidate
    expected_checkpoint: LegacyImportCheckpoint
    write_identity: LegacyImportWriteIdentity
    native_wins_required: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "plan_digest",
            _sha256(
                self.plan_digest,
                "legacy_work_plan_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "candidate_digest",
            _sha256(
                self.candidate_digest,
                "legacy_work_candidate_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "ordinal",
            _strict_non_negative_int(
                self.ordinal,
                "legacy_work_ordinal_invalid",
            ),
        )
        if not isinstance(self.candidate, LegacyImportCandidate):
            raise LegacyImportContractError("legacy_work_candidate_invalid")
        if not isinstance(self.expected_checkpoint, LegacyImportCheckpoint):
            raise LegacyImportContractError(
                "legacy_work_checkpoint_invalid"
            )
        if not isinstance(self.write_identity, LegacyImportWriteIdentity):
            raise LegacyImportContractError(
                "legacy_work_write_identity_invalid"
            )
        if (
            self.expected_checkpoint.board_id
            != self.candidate.key.board_id
            or self.expected_checkpoint.plan_digest != self.plan_digest
            or self.expected_checkpoint.candidate_digest
            != self.candidate_digest
            or self.expected_checkpoint.processed_count != self.ordinal
        ):
            raise LegacyImportContractError(
                "legacy_work_checkpoint_mismatch"
            )
        if self.native_wins_required is not True:
            raise LegacyImportContractError(
                "legacy_work_native_wins_required"
            )


@dataclass(frozen=True, slots=True)
class LegacyImportCandidateOutcome:
    work: LegacyImportCandidateWork
    resolution: LegacyImportResolution
    receipt_id: str
    checkpoint: LegacyImportCheckpoint
    atomic_resolution_applied: bool

    def __post_init__(self) -> None:
        if not isinstance(self.work, LegacyImportCandidateWork):
            raise LegacyImportContractError("legacy_outcome_work_invalid")
        if not isinstance(self.resolution, LegacyImportResolution):
            raise LegacyImportContractError(
                "legacy_outcome_resolution_invalid"
            )
        object.__setattr__(
            self,
            "receipt_id",
            _required_text(
                self.receipt_id,
                "legacy_outcome_receipt_id_required",
            ),
        )
        if not isinstance(self.checkpoint, LegacyImportCheckpoint):
            raise LegacyImportContractError(
                "legacy_outcome_checkpoint_invalid"
            )
        expected_imported = (
            self.work.expected_checkpoint.imported_count
            + int(self.resolution is LegacyImportResolution.IMPORTED)
        )
        expected_native = (
            self.work.expected_checkpoint.native_wins_count
            + int(self.resolution is LegacyImportResolution.NATIVE_WINS)
        )
        if (
            self.checkpoint.board_id
            != self.work.expected_checkpoint.board_id
            or self.checkpoint.plan_digest != self.work.plan_digest
            or self.checkpoint.candidate_digest
            != self.work.candidate_digest
            or self.checkpoint.processed_count != self.work.ordinal + 1
            or self.checkpoint.cursor_ordinal != self.work.ordinal
            or self.checkpoint.last_candidate_key != self.work.candidate.key
            or self.checkpoint.imported_count != expected_imported
            or self.checkpoint.native_wins_count != expected_native
        ):
            raise LegacyImportContractError(
                "legacy_outcome_checkpoint_mismatch"
            )
        if self.receipt_id != self.work.write_identity.receipt_id and (
            self.resolution is LegacyImportResolution.IMPORTED
        ):
            raise LegacyImportContractError(
                "legacy_outcome_import_receipt_identity_mismatch"
            )
        if self.atomic_resolution_applied is not True:
            raise LegacyImportContractError(
                "legacy_outcome_atomic_resolution_required"
            )


@dataclass(frozen=True, slots=True)
class LegacyImportCompletionExpectation:
    board_id: str
    epoch: str
    plan_digest: str
    candidate_digest: str
    candidate_count: int
    checkpoint: LegacyImportCheckpoint

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "legacy_completion_board_id_required",
            ),
        )
        if self.epoch != QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH:
            raise LegacyImportContractError(
                "legacy_completion_epoch_invalid"
            )
        object.__setattr__(
            self,
            "plan_digest",
            _sha256(
                self.plan_digest,
                "legacy_completion_plan_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "candidate_digest",
            _sha256(
                self.candidate_digest,
                "legacy_completion_candidate_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "candidate_count",
            _strict_non_negative_int(
                self.candidate_count,
                "legacy_completion_candidate_count_invalid",
            ),
        )
        if (
            not isinstance(self.checkpoint, LegacyImportCheckpoint)
            or self.checkpoint.board_id != self.board_id
            or self.checkpoint.plan_digest != self.plan_digest
            or self.checkpoint.candidate_digest != self.candidate_digest
            or self.checkpoint.processed_count != self.candidate_count
        ):
            raise LegacyImportContractError(
                "legacy_completion_checkpoint_mismatch"
            )


@dataclass(frozen=True, slots=True)
class LegacyImportPostcondition:
    expectation: LegacyImportCompletionExpectation
    all_candidates_resolved: bool
    unique_identity_satisfied: bool
    checkpoint_consistent: bool
    zero_orphans: bool
    audit_bundles_consistent: bool
    epoch_closed: bool
    completed_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(
            self.expectation,
            LegacyImportCompletionExpectation,
        ):
            raise LegacyImportContractError(
                "legacy_postcondition_expectation_invalid"
            )
        for field_name in (
            "all_candidates_resolved",
            "unique_identity_satisfied",
            "checkpoint_consistent",
            "zero_orphans",
            "audit_bundles_consistent",
            "epoch_closed",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise LegacyImportContractError(
                    f"legacy_postcondition_{field_name}_invalid"
                )
        object.__setattr__(
            self,
            "completed_at",
            _aware_utc(
                self.completed_at,
                "legacy_postcondition_completed_at_must_be_aware",
            ),
        )

    @property
    def satisfied(self) -> bool:
        return all(
            (
                self.all_candidates_resolved,
                self.unique_identity_satisfied,
                self.checkpoint_consistent,
                self.zero_orphans,
                self.audit_bundles_consistent,
                self.epoch_closed,
            )
        )


@dataclass(frozen=True, slots=True)
class LegacyImportRunRequest:
    board_id: str
    dry_run: bool = False
    cutoff: datetime | None = None
    epoch: str = QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "legacy_run_board_id_required",
            ),
        )
        if not isinstance(self.dry_run, bool):
            raise LegacyImportContractError("legacy_run_dry_run_invalid")
        if self.cutoff is not None:
            object.__setattr__(
                self,
                "cutoff",
                _aware_utc(
                    self.cutoff,
                    "legacy_run_cutoff_must_be_aware",
                ),
            )
        if self.epoch != QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH:
            raise LegacyImportContractError("legacy_import_epoch_unsupported")


@dataclass(frozen=True, slots=True)
class LegacyImportRunResult:
    plan: LegacyImportPlan
    selections: tuple[LegacyImportSelection, ...]
    dry_run: bool
    checkpoint: LegacyImportCheckpoint | None = None
    postcondition: LegacyImportPostcondition | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.plan, LegacyImportPlan):
            raise LegacyImportContractError("legacy_result_plan_invalid")
        object.__setattr__(self, "selections", tuple(self.selections))
        if any(
            not isinstance(item, LegacyImportSelection)
            for item in self.selections
        ):
            raise LegacyImportContractError(
                "legacy_result_selection_invalid"
            )
        if not isinstance(self.dry_run, bool):
            raise LegacyImportContractError("legacy_result_dry_run_invalid")
        if self.dry_run:
            if self.checkpoint is not None or self.postcondition is not None:
                raise LegacyImportContractError(
                    "legacy_dry_run_write_evidence_forbidden"
                )
        elif (
            not isinstance(self.checkpoint, LegacyImportCheckpoint)
            or not isinstance(self.postcondition, LegacyImportPostcondition)
            or not self.postcondition.satisfied
        ):
            raise LegacyImportContractError(
                "legacy_run_completion_evidence_required"
            )


def ideation_ambiguity_scale() -> AssessmentScale:
    return AssessmentScale(
        kind=AssessmentScaleKind.AMBIGUITY_SCORE,
        minimum=1,
        maximum=5,
        direction=ScoreDirection.LOWER_BETTER,
    )


def spec_validation_ambiguity_scale() -> AssessmentScale:
    return AssessmentScale(
        kind=AssessmentScaleKind.PERCENTAGE,
        minimum=0,
        maximum=100,
        direction=ScoreDirection.LOWER_BETTER,
    )


def strict_legacy_ideation_score(value: object) -> int | None:
    """Mirror the historical 1..5 parser while rejecting bool explicitly."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        score = value
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        score = int(value)
    elif isinstance(value, str) and value.strip().lstrip("-").isdigit():
        score = int(value.strip())
    else:
        return None
    return score if 1 <= score <= 5 else None


def strict_legacy_spec_score(value: object) -> float | None:
    """Accept only finite numeric 0..100 values; bool and strings are invalid."""

    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(float(value))
    ):
        return None
    score = float(value)
    return score if 0.0 <= score <= 100.0 else None
