"""Public persistence boundary for ``guideline-domain/v1``.

Adapters implement this Protocol inside the caller-owned transaction.  The
port never commits, rolls back, closes a transaction, opens a unit of work, or
depends on a transport/persistence framework.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.guideline_compliance import (
    GuidelineImpactItemPage,
    POLICY_FINDING_ORDERING,
    POLICY_IMPACT_ORDERING,
    POLICY_KEYSET_CONTRACT_VERSION,
    POLICY_RECEIPT_ORDERING,
    POLICY_WAIVER_ORDERING,
    PolicyComplianceCurrentSnapshot,
    PolicyComplianceFindingPage,
    PolicyComplianceReceiptPage,
    PolicyFindingPageCursor,
    PolicyImpactPageCursor,
    PolicyProjection,
    PolicyReceiptPageCursor,
    PolicyWaiverPage,
    PolicyWaiverPageCursor,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_PAGE_LIMIT_MAX,
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_ENTITY_TYPE_MAX_LENGTH,
    POLICY_FINDING_ID_MAX_LENGTH,
    POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH,
    BoardGuidelineBinding,
    Guideline,
    GuidelineHead,
    GuidelineImpactItemKind,
    GuidelineImpactReceipt,
    GuidelineRetirement,
    GuidelineRevision,
    GuidelineRevisionPage,
    GuidelineRevisionPageCursor,
    PolicyComplianceReceipt,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationOutcome,
    PolicyEvaluationResult,
    PolicySubjectRef,
    PolicySubjectSnapshot,
    PolicyWaiver,
    PolicyWaiverAuthorization,
    PolicyWaiverEvent,
    PolicyWaiverStatus,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    POLICY_RULE_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    POLICY_SUBJECT_ID_MAX_LENGTH,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_impact import (
    GuidelineAdoptionMutation,
    GuidelineImpactPreviewPlan,
    GuidelineUnlinkMutation,
)
from okto_pulse.core.domain.guideline_import_export import (
    GuidelineExportSnapshot,
    GuidelineImportPlan,
)
from okto_pulse.core.domain.guideline_policy_transition import (
    PolicyTransitionSnapshot,
)
from okto_pulse.core.domain.guideline_waiver_lifecycle import (
    PolicyWaiverMutation,
    PolicyWaiverSource,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


class GuidelinePolicyPersistenceError(RuntimeError):
    """Base transport-neutral persistence failure."""

    code = "guideline_policy_persistence_error"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: tuple[tuple[str, str], ...] = (),
    ) -> None:
        if not isinstance(details, tuple) or any(
            not isinstance(item, tuple)
            or len(item) != 2
            or not all(isinstance(value, str) for value in item)
            for item in details
        ):
            raise ValueError("guideline_policy_error_details_invalid")
        self.details = tuple(sorted(details))
        super().__init__(message or self.code)


class GuidelinePolicyAdapterMissing(GuidelinePolicyPersistenceError):
    code = "guideline_policy_adapter_missing"


class GuidelinePolicyHeadConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_head_conflict"


class GuidelinePolicyCasConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_cas_conflict"


class GuidelinePolicyRevisionConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_revision_conflict"


class GuidelinePolicyBindingConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_binding_conflict"


class GuidelinePolicySubjectConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_subject_conflict"


class GuidelinePolicyVersionConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_version_conflict"


class GuidelinePolicyDigestConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_digest_conflict"


class GuidelinePolicyIdempotencyConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_idempotency_conflict"


class GuidelinePolicyCursorConflict(GuidelinePolicyPersistenceError):
    code = "guideline_policy_cursor_conflict"


class GuidelinePolicyInvalidCursor(GuidelinePolicyCursorConflict):
    code = "invalid_cursor"


@dataclass(frozen=True, slots=True)
class GuidelineAdoptionReplay:
    """Canonical persisted result returned for an idempotent adoption replay."""

    binding: BoardGuidelineBinding
    receipt: GuidelineImpactReceipt
    actor_type: str
    event_id: str
    activity_id: str
    activity_action: str
    occurred_at: datetime
    request_digest: str


@dataclass(frozen=True, slots=True)
class GuidelineImpactPreviewReplay:
    """Exact persisted preview receipt plus its caller-request digest."""

    receipt: GuidelineImpactReceipt
    request_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, GuidelineImpactReceipt):
            raise ValueError("guideline_impact_preview_replay_receipt_invalid")
        object.__setattr__(
            self,
            "request_digest",
            _request_digest(
                self.request_digest,
                "guideline_impact_preview_replay_digest_invalid",
            ),
        )


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _request_digest(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise ValueError(code)
    normalized = value.strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ValueError(code)
    return normalized


@dataclass(frozen=True, slots=True)
class GuidelineRevisionReplay:
    """Immutable application replay evidence for one published revision."""

    revision: GuidelineRevision
    published_head: GuidelineHead
    request_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.revision, GuidelineRevision)
            or not isinstance(self.published_head, GuidelineHead)
            or self.revision.guideline_id != self.published_head.guideline_id
            or self.revision.revision_id != self.published_head.revision_id
            or self.revision.revision_number
            != self.published_head.revision_number
            or self.revision.semantic_version
            != self.published_head.semantic_version
        ):
            raise ValueError("guideline_revision_replay_bundle_invalid")
        object.__setattr__(
            self,
            "request_digest",
            _request_digest(
                self.request_digest,
                "guideline_revision_replay_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class GuidelineRevisionNoopReplay:
    """Durable no-op fence returned independently of the live guideline head.

    A no-op still consumes its idempotency key.  Persisting the original
    revision/head snapshot prevents a later retry from being replanned against
    a newer head and accidentally turning the original no-op into a write.
    """

    revision: GuidelineRevision
    original_head: GuidelineHead
    request_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.revision, GuidelineRevision)
            or not isinstance(self.original_head, GuidelineHead)
            or self.revision.guideline_id != self.original_head.guideline_id
            or self.revision.revision_id != self.original_head.revision_id
            or self.revision.revision_number
            != self.original_head.revision_number
            or self.revision.semantic_version
            != self.original_head.semantic_version
        ):
            raise ValueError("guideline_revision_noop_replay_bundle_invalid")
        object.__setattr__(
            self,
            "request_digest",
            _request_digest(
                self.request_digest,
                "guideline_revision_noop_replay_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class GuidelineRetirementReplay:
    """Immutable application replay evidence for one terminal mutation."""

    retirement: GuidelineRetirement
    request_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.retirement, GuidelineRetirement):
            raise ValueError("guideline_retirement_replay_bundle_invalid")
        object.__setattr__(
            self,
            "request_digest",
            _request_digest(
                self.request_digest,
                "guideline_retirement_replay_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class GuidelineDefaultMaterializationProof:
    """Exact template evidence required by the rev-1 default exception."""

    template_id: str
    template_version: int
    guideline_revision_number: int

    def __post_init__(self) -> None:
        for field_name in ("template_id",):
            try:
                value = normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=GUIDELINE_ID_MAX_LENGTH,
                    code="guideline_default_materialization_proof_invalid",
                )
            except ValueError:
                raise ValueError("guideline_default_materialization_proof_invalid")
            object.__setattr__(self, field_name, value)
        for field_name in (
            "template_version",
            "guideline_revision_number",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 1
                or value > POLICY_SQL_INTEGER_MAX
            ):
                raise ValueError("guideline_default_materialization_proof_invalid")


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(code)
    return value.strip()


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def _bounded_text(value: object, max_length: int, code: str) -> str:
    return normalize_policy_bounded_text(
        value,
        max_length=max_length,
        code=code,
    )


def _bounded_optional_text(
    value: object,
    max_length: int,
    code: str,
) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, max_length, code)


def _limit(value: object) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= GUIDELINE_PAGE_LIMIT_MAX
    ):
        raise ValueError("guideline_page_limit_invalid")
    return value


def _revision_cursor(
    value: object,
) -> GuidelineRevisionPageCursor | None:
    if value is not None and not isinstance(
        value,
        GuidelineRevisionPageCursor,
    ):
        raise ValueError("guideline_revision_cursor_invalid")
    return value


@dataclass(frozen=True, slots=True)
class GuidelineRevisionListQuery:
    guideline_id: str
    limit: int = 50
    cursor: GuidelineRevisionPageCursor | None = None
    projection: PolicyProjection = PolicyProjection.SUMMARY
    filter_digest: str = field(init=False)
    projection_digest: str = field(init=False)

    ordering = ("revision_number DESC", "revision_id DESC")

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "guideline_id",
            _bounded_text(
                self.guideline_id,
                GUIDELINE_ID_MAX_LENGTH,
                "guideline_id_required",
            ),
        )
        object.__setattr__(self, "limit", _limit(self.limit))
        if not isinstance(self.projection, PolicyProjection):
            raise ValueError("guideline_revision_projection_invalid")
        filter_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "revision",
                "guideline_id": self.guideline_id,
            }
        )
        projection_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "revision",
                "projection": self.projection.value,
            }
        )
        object.__setattr__(self, "filter_digest", filter_digest)
        object.__setattr__(self, "projection_digest", projection_digest)
        object.__setattr__(
            self,
            "cursor",
            _revision_cursor(self.cursor),
        )
        if self.cursor is not None and (
            self.cursor.filter_digest != filter_digest
            or self.cursor.projection_digest != projection_digest
        ):
            raise GuidelinePolicyInvalidCursor(
                "guideline_revision_cursor_context_mismatch"
            )


@dataclass(frozen=True, slots=True)
class GuidelineImpactListQuery:
    board_id: str
    impact_receipt_id: str
    limit: int = 50
    cursor: PolicyImpactPageCursor | None = None
    guideline_id: str | None = None
    entity_type: str | None = None
    item_kind: GuidelineImpactItemKind | None = None
    projection: PolicyProjection = PolicyProjection.SUMMARY
    filter_digest: str = field(init=False)
    projection_digest: str = field(init=False)

    ordering = POLICY_IMPACT_ORDERING

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "guideline_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "impact_receipt_id",
            _bounded_text(
                self.impact_receipt_id,
                POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH,
                "guideline_impact_receipt_id_required",
            ),
        )
        object.__setattr__(
            self,
            "guideline_id",
            _bounded_optional_text(
                self.guideline_id,
                GUIDELINE_ID_MAX_LENGTH,
                "guideline_impact_guideline_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "entity_type",
            _bounded_optional_text(
                self.entity_type,
                POLICY_ENTITY_TYPE_MAX_LENGTH,
                "guideline_impact_entity_type_invalid",
            ),
        )
        object.__setattr__(self, "limit", _limit(self.limit))
        if self.item_kind is not None and not isinstance(
            self.item_kind,
            GuidelineImpactItemKind,
        ):
            raise ValueError("guideline_impact_item_kind_invalid")
        if not isinstance(self.projection, PolicyProjection):
            raise ValueError("guideline_impact_projection_invalid")
        filter_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "impact",
                "board_id": self.board_id,
                "impact_receipt_id": self.impact_receipt_id,
                "guideline_id": self.guideline_id,
                "entity_type": self.entity_type,
                "item_kind": (
                    self.item_kind.value if self.item_kind is not None else None
                ),
            }
        )
        projection_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "impact",
                "projection": self.projection.value,
            }
        )
        object.__setattr__(self, "filter_digest", filter_digest)
        object.__setattr__(self, "projection_digest", projection_digest)
        if self.cursor is not None:
            if not isinstance(self.cursor, PolicyImpactPageCursor):
                raise ValueError("guideline_impact_cursor_invalid")
            if (
                self.cursor.filter_digest != filter_digest
                or self.cursor.projection_digest != projection_digest
            ):
                raise GuidelinePolicyInvalidCursor(
                    "guideline_impact_cursor_context_mismatch"
                )


@dataclass(frozen=True, slots=True)
class PolicyComplianceReceiptListQuery:
    board_id: str
    limit: int = 50
    cursor: PolicyReceiptPageCursor | None = None
    entity_type: PolicyEntityType | None = None
    subject_id: str | None = None
    outcome: PolicyEvaluationOutcome | None = None
    currentness: PolicyCurrentness | None = None
    projection: PolicyProjection = PolicyProjection.SUMMARY
    filter_digest: str = field(init=False)
    projection_digest: str = field(init=False)

    ordering = POLICY_RECEIPT_ORDERING

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "policy_receipt_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "subject_id",
            _bounded_optional_text(
                self.subject_id,
                POLICY_SUBJECT_ID_MAX_LENGTH,
                "policy_receipt_subject_id_invalid",
            ),
        )
        object.__setattr__(self, "limit", _limit(self.limit))
        if self.entity_type is not None:
            if not isinstance(self.entity_type, PolicyEntityType):
                raise ValueError("policy_receipt_entity_type_invalid")
        if self.outcome is not None and not isinstance(
            self.outcome,
            PolicyEvaluationOutcome,
        ):
            raise ValueError("policy_receipt_outcome_invalid")
        if self.currentness is not None and not isinstance(
            self.currentness,
            PolicyCurrentness,
        ):
            raise ValueError("policy_receipt_currentness_invalid")
        if not isinstance(self.projection, PolicyProjection):
            raise ValueError("policy_receipt_projection_invalid")
        filter_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "receipt",
                "board_id": self.board_id,
                "entity_type": (
                    self.entity_type.value if self.entity_type is not None else None
                ),
                "subject_id": self.subject_id,
                "outcome": (self.outcome.value if self.outcome is not None else None),
                "currentness": (
                    self.currentness.value if self.currentness is not None else None
                ),
            }
        )
        projection_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "receipt",
                "projection": self.projection.value,
            }
        )
        object.__setattr__(self, "filter_digest", filter_digest)
        object.__setattr__(self, "projection_digest", projection_digest)
        if self.cursor is not None:
            if not isinstance(self.cursor, PolicyReceiptPageCursor):
                raise ValueError("policy_receipt_cursor_invalid")
            if (
                self.cursor.filter_digest != filter_digest
                or self.cursor.projection_digest != projection_digest
            ):
                raise GuidelinePolicyInvalidCursor(
                    "policy_receipt_cursor_context_mismatch"
                )


@dataclass(frozen=True, slots=True)
class PolicyComplianceFindingListQuery:
    board_id: str
    limit: int = 50
    cursor: PolicyFindingPageCursor | None = None
    receipt_id: str | None = None
    guideline_id: str | None = None
    rule_id: str | None = None
    subject_id: str | None = None
    outcome: PolicyEvaluationOutcome | None = None
    projection: PolicyProjection = PolicyProjection.SUMMARY
    filter_digest: str = field(init=False)
    projection_digest: str = field(init=False)

    ordering = POLICY_FINDING_ORDERING

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "policy_finding_board_id_required",
            ),
        )
        for field_name, max_length in (
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("rule_id", POLICY_RULE_ID_MAX_LENGTH),
            ("subject_id", POLICY_SUBJECT_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_optional_text(
                    getattr(self, field_name),
                    max_length,
                    f"policy_finding_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "limit", _limit(self.limit))
        if self.outcome is not None and not isinstance(
            self.outcome,
            PolicyEvaluationOutcome,
        ):
            raise ValueError("policy_finding_outcome_invalid")
        if not isinstance(self.projection, PolicyProjection):
            raise ValueError("policy_finding_projection_invalid")
        filter_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "finding",
                "board_id": self.board_id,
                "receipt_id": self.receipt_id,
                "guideline_id": self.guideline_id,
                "rule_id": self.rule_id,
                "subject_id": self.subject_id,
                "outcome": (self.outcome.value if self.outcome is not None else None),
            }
        )
        projection_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "finding",
                "projection": self.projection.value,
            }
        )
        object.__setattr__(self, "filter_digest", filter_digest)
        object.__setattr__(self, "projection_digest", projection_digest)
        if self.cursor is not None:
            if not isinstance(self.cursor, PolicyFindingPageCursor):
                raise ValueError("policy_finding_cursor_invalid")
            if (
                self.cursor.filter_digest != filter_digest
                or self.cursor.projection_digest != projection_digest
            ):
                raise GuidelinePolicyInvalidCursor(
                    "policy_finding_cursor_context_mismatch"
                )


@dataclass(frozen=True, slots=True)
class PolicyWaiverListQuery:
    board_id: str
    evaluated_at: datetime
    limit: int = 50
    cursor: PolicyWaiverPageCursor | None = None
    finding_id: str | None = None
    receipt_id: str | None = None
    guideline_id: str | None = None
    revision_id: str | None = None
    rule_id: str | None = None
    entity_type: PolicyEntityType | None = None
    subject_id: str | None = None
    subject_version: int | None = None
    status: PolicyWaiverStatus | None = None
    projection: PolicyProjection = PolicyProjection.SUMMARY
    filter_digest: str = field(init=False)
    projection_digest: str = field(init=False)

    ordering = POLICY_WAIVER_ORDERING

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "policy_waiver_board_id_required",
            ),
        )
        if (
            not isinstance(self.evaluated_at, datetime)
            or self.evaluated_at.tzinfo is None
            or self.evaluated_at.utcoffset() is None
        ):
            raise ValueError("policy_waiver_evaluated_at_invalid")
        object.__setattr__(
            self,
            "evaluated_at",
            self.evaluated_at.astimezone(timezone.utc),
        )
        for field_name, max_length in (
            ("finding_id", POLICY_FINDING_ID_MAX_LENGTH),
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("rule_id", POLICY_RULE_ID_MAX_LENGTH),
            ("subject_id", POLICY_SUBJECT_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_optional_text(
                    getattr(self, field_name),
                    max_length,
                    f"policy_waiver_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "limit", _limit(self.limit))
        if self.entity_type is not None and not isinstance(
            self.entity_type,
            PolicyEntityType,
        ):
            raise ValueError("policy_waiver_entity_type_invalid")
        if self.subject_version is not None and (
            not isinstance(self.subject_version, int)
            or isinstance(self.subject_version, bool)
            or not 1 <= self.subject_version <= POLICY_SQL_INTEGER_MAX
        ):
            raise ValueError("policy_waiver_subject_version_invalid")
        if self.status is not None and not isinstance(
            self.status,
            PolicyWaiverStatus,
        ):
            raise ValueError("policy_waiver_status_invalid")
        if not isinstance(self.projection, PolicyProjection):
            raise ValueError("policy_waiver_projection_invalid")
        filter_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "waiver",
                "board_id": self.board_id,
                "evaluated_at": self.evaluated_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "finding_id": self.finding_id,
                "receipt_id": self.receipt_id,
                "guideline_id": self.guideline_id,
                "revision_id": self.revision_id,
                "rule_id": self.rule_id,
                "entity_type": (
                    self.entity_type.value if self.entity_type is not None else None
                ),
                "subject_id": self.subject_id,
                "subject_version": self.subject_version,
                "status": (self.status.value if self.status is not None else None),
            }
        )
        projection_digest = canonical_sha256(
            {
                "contract": POLICY_KEYSET_CONTRACT_VERSION,
                "kind": "waiver",
                "projection": self.projection.value,
            }
        )
        object.__setattr__(self, "filter_digest", filter_digest)
        object.__setattr__(self, "projection_digest", projection_digest)
        if self.cursor is not None:
            if not isinstance(self.cursor, PolicyWaiverPageCursor):
                raise ValueError("policy_waiver_cursor_invalid")
            if (
                self.cursor.filter_digest != filter_digest
                or self.cursor.projection_digest != projection_digest
            ):
                raise GuidelinePolicyInvalidCursor(
                    "policy_waiver_cursor_context_mismatch"
                )


@runtime_checkable
class PolicyComplianceCurrentSnapshotResolver(Protocol):
    """Server-owned live-fence resolver used for honest read projections."""

    async def resolve_current_snapshot(
        self,
        *,
        board_id: str,
        entity_type: PolicyEntityType,
        subject_id: str,
    ) -> PolicyComplianceCurrentSnapshot | None: ...


@runtime_checkable
class PolicyTransitionSnapshotResolver(Protocol):
    """Resolve one gate snapshot inside the caller-owned unit of work.

    Implementations used by mutation paths must apply their edition-specific
    board/subject locking before returning.  The core contract intentionally
    does not prescribe a database lock primitive.
    """

    async def resolve_transition_snapshot(
        self,
        *,
        board_id: str,
        entity_type: PolicyEntityType,
        subject_id: str,
        expected_from_status: str,
    ) -> PolicyTransitionSnapshot: ...


@runtime_checkable
class GuidelinePolicyPersistencePort(
    PolicyTransitionSnapshotResolver,
    Protocol,
):
    """Transaction-bound source of truth for versioned guideline policy.

    CAS methods must perform their complete mutation atomically in the
    transaction supplied by the application layer.  Implementations must not
    commit, roll back, close the transaction, or create a nested unit of work.
    """

    async def get_guideline(
        self,
        *,
        guideline_id: str,
    ) -> Guideline | None: ...

    async def get_head(
        self,
        *,
        guideline_id: str,
    ) -> GuidelineHead | None: ...

    async def get_revision(
        self,
        *,
        guideline_id: str,
        revision_id: str,
    ) -> GuidelineRevision | None: ...

    async def get_retirement(
        self,
        *,
        guideline_id: str,
    ) -> GuidelineRetirement | None: ...

    async def list_revisions(
        self,
        query: GuidelineRevisionListQuery,
    ) -> GuidelineRevisionPage: ...

    async def get_revision_result_by_idempotency(
        self,
        *,
        guideline_id: str,
        idempotency_key: str,
    ) -> GuidelineRevisionReplay | GuidelineRevisionNoopReplay | None:
        """Return the original published bundle before planning against live head."""

        ...

    async def get_retirement_result_by_idempotency(
        self,
        *,
        guideline_id: str,
        idempotency_key: str,
    ) -> GuidelineRetirementReplay | None:
        """Return the original terminal bundle before terminal-state planning."""

        ...

    async def create_guideline(
        self,
        *,
        guideline: Guideline,
        initial_revision: GuidelineRevision,
        initial_head: GuidelineHead,
        idempotency_key: str,
        request_digest: str,
    ) -> tuple[Guideline, GuidelineRevision, GuidelineHead]: ...

    async def append_revision_cas(
        self,
        *,
        revision: GuidelineRevision,
        next_head: GuidelineHead,
        expected_head_revision: int,
        idempotency_key: str,
        request_digest: str,
    ) -> tuple[GuidelineRevision, GuidelineHead]:
        """Append or replay one exact bundle in the shared revision-key namespace.

        Applied and no-op records share the same ``(guideline_id,
        idempotency_key)`` namespace.  Implementations must check both ledgers
        after locking guideline authority, and a concurrent applied replay may
        succeed only when the stored revision and published head equal the
        requested bundle (including ``revision_id``).
        """

        ...

    async def record_revision_noop_cas(
        self,
        *,
        replay: GuidelineRevisionNoopReplay,
        idempotency_key: str,
    ) -> GuidelineRevisionNoopReplay:
        """Consume a no-op key atomically while fencing its original head.

        Implementations must verify that the live head still equals
        ``replay.original_head`` and insert the immutable no-op ledger record
        in the caller-owned transaction.  The key shares a namespace with
        applied revisions; a concurrent key or head conflict must fail closed
        with a typed persistence conflict.
        """

        ...

    async def retire_guideline_cas(
        self,
        *,
        retirement: GuidelineRetirement,
        expected_head_revision: int,
        idempotency_key: str,
        request_digest: str,
        actor_type: str = "user",
    ) -> GuidelineRetirement: ...

    async def get_binding(
        self,
        *,
        board_id: str,
        guideline_id: str,
    ) -> BoardGuidelineBinding | None: ...

    async def list_bindings(
        self,
        *,
        board_id: str,
    ) -> tuple[BoardGuidelineBinding, ...]: ...

    async def append_binding_cas(
        self,
        *,
        binding: BoardGuidelineBinding,
        expected_binding_revision: int | None,
        idempotency_key: str,
        request_digest: str,
        materialization_proof: (GuidelineDefaultMaterializationProof | None) = None,
        actor_type: str = "user",
    ) -> BoardGuidelineBinding: ...

    async def save_impact_preview(
        self,
        *,
        plan: GuidelineImpactPreviewPlan,
    ) -> GuidelineImpactReceipt: ...

    async def adopt_revision_cas(
        self,
        *,
        mutation: GuidelineAdoptionMutation,
    ) -> tuple[BoardGuidelineBinding, GuidelineImpactReceipt]: ...

    async def unlink_binding_cas(
        self,
        *,
        mutation: GuidelineUnlinkMutation,
    ) -> BoardGuidelineBinding: ...

    async def get_impact_receipt(
        self,
        *,
        board_id: str,
        impact_receipt_id: str,
    ) -> GuidelineImpactReceipt | None: ...

    async def get_impact_receipt_by_idempotency(
        self,
        *,
        board_id: str,
        idempotency_key: str,
    ) -> GuidelineImpactPreviewReplay | None: ...

    async def get_adoption_result_by_idempotency(
        self,
        *,
        board_id: str,
        idempotency_key: str,
    ) -> GuidelineAdoptionReplay | None: ...

    async def list_impact_items(
        self,
        query: GuidelineImpactListQuery,
    ) -> GuidelineImpactItemPage: ...

    async def list_policy_subjects(
        self,
        *,
        board_id: str,
    ) -> tuple[PolicySubjectRef, ...]: ...

    async def resolve_policy_subject_snapshot(
        self,
        *,
        board_id: str,
        entity_type: PolicyEntityType,
        subject_id: str,
        lock: bool = False,
    ) -> PolicySubjectSnapshot | None:
        """Resolve one server-owned subject snapshot for policy evaluation."""

        ...

    async def list_board_waivers(
        self,
        *,
        board_id: str,
    ) -> tuple[PolicyWaiver, ...]: ...

    async def save_evaluation_result(
        self,
        *,
        result: PolicyEvaluationResult,
        current_snapshot: PolicyComplianceCurrentSnapshot,
        idempotency_key: str,
        request_digest: str,
    ) -> PolicyEvaluationResult: ...

    async def get_compliance_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
    ) -> PolicyComplianceReceipt | None: ...

    async def get_current_compliance_receipt(
        self,
        *,
        board_id: str,
        entity_type: PolicyEntityType,
        subject_id: str,
    ) -> PolicyComplianceReceipt | None: ...

    async def list_compliance_receipts(
        self,
        query: PolicyComplianceReceiptListQuery,
    ) -> PolicyComplianceReceiptPage: ...

    async def list_compliance_findings(
        self,
        query: PolicyComplianceFindingListQuery,
    ) -> PolicyComplianceFindingPage: ...

    async def get_waiver(
        self,
        *,
        board_id: str,
        waiver_id: str,
    ) -> PolicyWaiver | None: ...

    async def list_waivers(
        self,
        query: PolicyWaiverListQuery,
    ) -> PolicyWaiverPage: ...

    async def list_waiver_events(
        self,
        *,
        board_id: str,
        waiver_id: str,
    ) -> tuple[PolicyWaiverEvent, ...]: ...

    async def resolve_policy_waiver_source(
        self,
        *,
        board_id: str,
        finding_id: str,
        require_current: bool = True,
        lock: bool = False,
    ) -> PolicyWaiverSource | None:
        """Resolve immutable waiver evidence plus its live-fence assessment."""

        ...

    async def create_waiver(
        self,
        *,
        mutation: PolicyWaiverMutation,
        idempotency_key: str,
        request_digest: str,
    ) -> tuple[PolicyWaiver, PolicyWaiverEvent]: ...

    async def transition_waiver_cas(
        self,
        *,
        mutation: PolicyWaiverMutation,
        expected_waiver_revision: int,
        idempotency_key: str,
        request_digest: str,
    ) -> tuple[PolicyWaiver, PolicyWaiverEvent]: ...

    async def resolve_effective_waiver(
        self,
        *,
        board_id: str,
        guideline_id: str,
        revision_id: str,
        rule_id: str,
        entity_type: PolicyEntityType,
        subject_id: str,
        subject_version: int,
        evaluated_at: datetime,
    ) -> PolicyWaiverAuthorization | None: ...

    async def resolve_idempotent_result(
        self,
        *,
        operation: str,
        scope_id: str,
        idempotency_key: str,
        request_digest: str,
    ) -> object | None:
        """Return the exact original result or fail on key/digest drift."""

        ...

    async def export_guideline_snapshot(
        self,
        *,
        guideline_ids: tuple[str, ...] | None = None,
        owner_id: str | None = None,
        board_id: str | None = None,
        include_binding_history: bool = True,
    ) -> GuidelineExportSnapshot:
        """Read one canonical immutable policy snapshot."""

        ...

    async def load_guideline_import_snapshot(
        self,
        *,
        guideline_ids: tuple[str, ...],
    ) -> GuidelineExportSnapshot:
        """Load collision-complete state for import planning.

        Unlike public export selection, this trusted internal read spans
        owners and boards and tolerates missing identities.  Callers must
        never expose its payload directly through a transport.
        """

        ...

    async def apply_guideline_import_plan(
        self,
        plan: GuidelineImportPlan,
        *,
        imported_by: str,
        imported_at: datetime,
        import_digest: str,
    ) -> None:
        """Stage one validated import atomically in the caller transaction."""

        ...


__all__ = [
    "GuidelineAdoptionReplay",
    "GuidelineDefaultMaterializationProof",
    "GuidelineImpactListQuery",
    "GuidelineImpactPreviewReplay",
    "GuidelinePolicyAdapterMissing",
    "GuidelinePolicyBindingConflict",
    "GuidelinePolicyCasConflict",
    "GuidelinePolicyCursorConflict",
    "GuidelinePolicyDigestConflict",
    "GuidelinePolicyHeadConflict",
    "GuidelinePolicyIdempotencyConflict",
    "GuidelinePolicyInvalidCursor",
    "GuidelinePolicyPersistenceError",
    "GuidelinePolicyPersistencePort",
    "GuidelinePolicyRevisionConflict",
    "GuidelinePolicySubjectConflict",
    "GuidelinePolicyVersionConflict",
    "GuidelineRetirementReplay",
    "GuidelineRevisionNoopReplay",
    "GuidelineRevisionReplay",
    "GuidelineRevisionListQuery",
    "PolicyComplianceFindingListQuery",
    "PolicyComplianceCurrentSnapshotResolver",
    "PolicyComplianceReceiptListQuery",
    "PolicyTransitionSnapshotResolver",
    "PolicyWaiverListQuery",
]
