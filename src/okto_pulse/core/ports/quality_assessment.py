"""Edition-neutral persistence/read ports for SK-A quality assessments."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.quality_assessment import (
    AssessmentCommitResult,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentPreflight,
    AssessmentPreflightRequest,
    AssessmentReceipt,
    AssessmentReceiptDetail,
    AssessmentReceiptState,
    AssessmentReceiptView,
    AssessmentSubjectHead,
    AssessmentSubjectIdentity,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentSource,
    AssessmentSubmission,
    AssessmentWriteBundle,
    FindingSeverity,
    QualityFinding,
    QualityPage,
    QualityPageCursor,
)
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


class QualityAssessmentPersistenceError(RuntimeError):
    """Base transport-neutral error raised by a persistence adapter."""

    code = "quality_assessment_persistence_error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)


class QualityAssessmentAdapterMissing(QualityAssessmentPersistenceError):
    code = "quality_assessment_adapter_missing"


class AssessmentHeadRevisionConflict(QualityAssessmentPersistenceError):
    code = "assessment_head_revision_conflict"


class AssessmentSubjectVersionConflict(QualityAssessmentPersistenceError):
    code = "assessment_subject_version_conflict"


class AssessmentIdempotencyConflict(QualityAssessmentPersistenceError):
    code = "assessment_idempotency_conflict"


class AssessmentInputDigestConflict(QualityAssessmentPersistenceError):
    code = "assessment_input_digest_conflict"


class AssessmentSubjectStatusConflict(QualityAssessmentPersistenceError):
    code = "assessment_subject_status_conflict"


class AssessmentSubjectLifecycleConflict(QualityAssessmentPersistenceError):
    code = "assessment_subject_lifecycle_conflict"


class AssessmentAuthorityConflict(QualityAssessmentPersistenceError):
    code = "assessment_authority_conflict"


class AssessmentSubjectNotFound(QualityAssessmentPersistenceError):
    code = "assessment_subject_not_found"


class AssessmentReceiptNotFound(QualityAssessmentPersistenceError):
    code = "assessment_receipt_not_found"


class AssessmentReadAccessDenied(QualityAssessmentPersistenceError):
    code = "assessment_read_access_denied"


@dataclass(frozen=True, slots=True)
class AssessmentListQuery:
    subject: AssessmentSubjectIdentity
    offset: int
    limit: int
    assessment_kind: AssessmentKind | None = None
    state: AssessmentReceiptState | None = None
    current_subject_version: int | None = None
    current_digests: AssessmentDigestSet | None = None
    currentness_inputs: tuple["AssessmentCurrentnessInput", ...] = ()
    cursor: QualityPageCursor | None = None


@dataclass(frozen=True, slots=True)
class FindingListQuery:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    offset: int
    limit: int
    receipt_id: str | None = None
    assessment_kind: AssessmentKind | None = None
    category_code: str | None = None
    severity: FindingSeverity | None = None
    cursor: QualityPageCursor | None = None


@dataclass(frozen=True, slots=True)
class AssessmentCurrentnessInput:
    """Normative current digest selected by kind/origin/source identity."""

    assessment_kind: AssessmentKind
    origin: AssessmentOrigin
    source: AssessmentSource
    digests: AssessmentDigestSet


@dataclass(frozen=True, slots=True)
class QualityGateInput:
    """Edition-resolved gate settings consumed by the shared read projector."""

    assessment_kind: AssessmentKind
    applicable: bool
    enabled: bool
    threshold: float | None
    skipped: bool

    def __post_init__(self) -> None:
        if not isinstance(self.assessment_kind, AssessmentKind):
            raise ValueError("quality_gate_assessment_kind_invalid")
        for field_name in ("applicable", "enabled", "skipped"):
            if not isinstance(getattr(self, field_name), bool):
                raise ValueError(f"quality_gate_{field_name}_invalid")
        if self.threshold is not None and (
            not isinstance(self.threshold, int | float)
            or isinstance(self.threshold, bool)
        ):
            raise ValueError("quality_gate_threshold_invalid")


@dataclass(frozen=True, slots=True)
class QualityAssessmentReadContext:
    """Authorized current subject plus all supported digest identities."""

    subject: AssessmentSubjectRef
    currentness_inputs: tuple[AssessmentCurrentnessInput, ...]
    gate_inputs: tuple[QualityGateInput, ...] = ()


@runtime_checkable
class QualityAssessmentPreflightReadPort(Protocol):
    """Resolve a read-only fence before any writable UoW is opened."""

    async def lookup_assessment_replay(
        self,
        *,
        board_id: str,
        idempotency_key: str,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> AssessmentCommitResult | None:
        """Return a board-scoped prior result without revealing another subject."""
        ...

    async def resolve_assessment_preflight(
        self,
        submission: AssessmentSubmission,
        *,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> AssessmentPreflight:
        ...

    async def resolve_assessment_preflight_request(
        self,
        request: AssessmentPreflightRequest,
        *,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> AssessmentPreflight:
        """Resolve server-owned assessment inputs without a materialized submission."""
        ...

    async def resolve_assessment_read_context(
        self,
        *,
        board_id: str,
        subject_type: AssessmentSubjectType,
        subject_id: str,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> QualityAssessmentReadContext:
        """Authorize a literal subject and rederive its current digest catalog."""
        ...

    async def resolve_receipt_read_context(
        self,
        *,
        receipt_id: str,
        board_id: str | None,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> QualityAssessmentReadContext:
        """Resolve receipt ownership without leaking cross-board existence."""
        ...


@runtime_checkable
class QualityAssessmentPersistencePort(Protocol):
    """Transaction-bound relational source of truth.

    ``apply_bundle_cas`` must first repeat idempotency lookup in the transaction,
    then revalidate row existence, ``archived=false``, status, subject version,
    input digest, authority digest, reviewer separation, head revision, and
    predecessor receipt identity against the bundle's complete fence.
    It stages receipt, findings, questions, links, head, history, event, and
    typed ``bundle.audit_intent`` history/event/outbox identities in the
    caller's transaction and never commits internally.  The result must echo
    those identities, so atomic evidence is structurally verifiable.  A
    matching key + request fingerprint returns the original result; a different
    fingerprint raises :class:`AssessmentIdempotencyConflict`.  All fences are
    database predicates/CAS, never unguarded select-then-write. Implementations
    persist the receipt's scale kind, analyzer version, source/channel,
    idempotency/request provenance, predecessor, and outcome verbatim so later
    receipt reads remain self-describing.
    """

    async def apply_bundle_cas(
        self,
        bundle: AssessmentWriteBundle,
    ) -> AssessmentCommitResult:
        ...

    async def get_current(
        self,
        *,
        board_id: str,
        subject_type: AssessmentSubjectType,
        subject_id: str,
        assessment_kind: AssessmentKind,
    ) -> tuple[AssessmentReceipt, AssessmentSubjectHead] | None:
        ...

    async def get_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
    ) -> AssessmentReceipt | None:
        ...

    async def get_receipt_detail(
        self,
        *,
        board_id: str,
        receipt_id: str,
    ) -> AssessmentReceiptDetail | None:
        ...

    async def list_assessments(
        self,
        query: AssessmentListQuery,
    ) -> QualityPage[AssessmentReceiptView]:
        ...

    async def list_findings(
        self,
        query: FindingListQuery,
    ) -> QualityPage[QualityFinding]:
        ...


_PREFLIGHT_READER_RUNTIME_KEY = "ports.quality_assessment.preflight_reader"


def register_quality_assessment_preflight_reader(
    reader: QualityAssessmentPreflightReadPort,
) -> None:
    register_runtime_value(_PREFLIGHT_READER_RUNTIME_KEY, reader)


def get_quality_assessment_preflight_reader(
) -> QualityAssessmentPreflightReadPort:
    return require_runtime_value(
        _PREFLIGHT_READER_RUNTIME_KEY,
        "quality_assessment_preflight_reader_not_configured",
    )


def reset_quality_assessment_preflight_reader_for_tests() -> None:
    reset_runtime_values(_PREFLIGHT_READER_RUNTIME_KEY)


__all__ = [
    "AssessmentAuthorityConflict",
    "AssessmentHeadRevisionConflict",
    "AssessmentIdempotencyConflict",
    "AssessmentInputDigestConflict",
    "AssessmentListQuery",
    "AssessmentCurrentnessInput",
    "AssessmentReadAccessDenied",
    "AssessmentReceiptNotFound",
    "AssessmentSubjectVersionConflict",
    "AssessmentSubjectNotFound",
    "AssessmentSubjectStatusConflict",
    "AssessmentSubjectLifecycleConflict",
    "FindingListQuery",
    "QualityAssessmentPersistenceError",
    "QualityAssessmentAdapterMissing",
    "QualityAssessmentPersistencePort",
    "QualityAssessmentPreflightReadPort",
    "QualityAssessmentReadContext",
    "QualityGateInput",
    "get_quality_assessment_preflight_reader",
    "register_quality_assessment_preflight_reader",
    "reset_quality_assessment_preflight_reader_for_tests",
]
