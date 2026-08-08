"""Application boundary for semantic guideline reads and exceptions.

This module owns the semantic/v2 public contract while the older
``policy_governance`` module keeps revision/adoption compatibility.  It never
opens a transaction and never accepts server-owned actors, IDs, timestamps or
digests from an inbound adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    ASSESSMENTS_READ,
    WAIVER_READ,
    WAIVER_REQUEST,
    WAIVER_REVALIDATE,
    WAIVER_REVIEW,
    WAIVER_REVOKE,
    Clock,
    IdFactory,
    _aware_utc,
    _require_board,
    _require_capability,
    _uuid5,
    _utc_now,
    _write,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_FINDING_ID_MAX_LENGTH,
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    POLICY_SUBJECT_ID_MAX_LENGTH,
    BoardGuidelineBinding,
    GuidelineBindingState,
    GuidelineRevision,
    PolicyCurrentness,
    PolicyEntityType,
    PolicySubjectSnapshot,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentSnapshot,
    SemanticAssessmentCurrentness,
    SemanticAssessmentCurrentnessReason,
    assess_semantic_assessment_currentness,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticAssessmentContractError,
    SemanticExceptionActorKind,
    SemanticMetricWaiver,
    SemanticMetricWaiverAnchor,
    SemanticMetricWaiverEvent,
    SemanticMetricWaiverEventType,
    SemanticMetricWaiverExpireReason,
    SemanticMetricWaiverMutation,
    SemanticMetricWaiverRevalidationReason,
    SemanticMetricWaiverRevalidationStatus,
    SemanticMetricWaiverStatus,
    SemanticPolicySkip,
    SemanticPolicySkipEventType,
    SemanticPolicySkipMutation,
    SemanticPolicySkipScope,
    create_semantic_policy_skip,
    request_semantic_metric_waiver,
    revalidate_semantic_metric_waiver,
    revoke_semantic_policy_skip,
    transition_semantic_metric_waiver,
)
from okto_pulse.core.domain.guideline_semantic_projection import (
    SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX,
    SemanticAssessmentPage,
    SemanticAssessmentPageCursor,
    SemanticAssessmentProjection,
    SemanticFindingPage,
    SemanticFindingPageCursor,
    SemanticFindingProjection,
    SemanticGuidelineProjection,
    SemanticSkipPage,
    SemanticSkipPageCursor,
    SemanticSkipProjection,
    SemanticWaiverPage,
    SemanticWaiverPageCursor,
    SemanticWaiverProjection,
    project_semantic_assessment,
    project_semantic_finding,
    project_semantic_skip,
    project_semantic_waiver,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    semantic_metric_result_digest_v1,
)
from okto_pulse.core.domain.quality_assessment import EvidenceRef
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyIdempotencyConflict,
    SemanticAssessmentListQuery,
    SemanticFindingListQuery,
    SemanticGuidelineAssessmentPersistencePort,
    SemanticSkipListQuery,
    SemanticWaiverListQuery,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


def _bounded(value: object, max_length: int, code: str) -> str:
    return normalize_policy_bounded_text(
        value,
        max_length=max_length,
        code=code,
    )


def _profile(value: object) -> SemanticGuidelineProjection:
    if not isinstance(value, SemanticGuidelineProjection):
        raise ValueError("semantic_guideline_projection_invalid")
    return value


def _evidence(value: object) -> tuple[EvidenceRef, ...]:
    if (
        not isinstance(value, tuple | list)
        or not value
        or any(not isinstance(item, EvidenceRef) for item in value)
    ):
        raise ValueError("semantic_waiver_evidence_required")
    resolved = tuple(value)
    if len(set(resolved)) != len(resolved):
        raise ValueError("semantic_waiver_evidence_duplicate")
    return tuple(
        sorted(
            resolved,
            key=lambda item: (
                item.source_type,
                item.source_id,
                item.source_version,
                item.content_hash,
            ),
        )
    )


async def _semantic_port(
    uow: PulseUnitOfWork,
) -> SemanticGuidelineAssessmentPersistencePort:
    return uow.services.guidelines.semantic_policy_persistence()


async def _receipt_currentness(
    semantic_port: SemanticGuidelineAssessmentPersistencePort,
    receipt: Any,
) -> SemanticAssessmentCurrentness:
    current = await semantic_port.resolve_semantic_assessment_current_snapshot(
        board_id=receipt.subject.board_id,
        entity_type=receipt.subject.entity_type,
        subject_id=receipt.subject.subject_id,
        binding_id=receipt.binding_id,
        lock=False,
    )
    return assess_semantic_assessment_currentness(receipt, current)


async def _finding_currentness(
    semantic_port: SemanticGuidelineAssessmentPersistencePort,
    *,
    board_id: str,
    receipt_id: str,
) -> SemanticAssessmentCurrentness:
    receipt = await semantic_port.get_semantic_assessment_receipt(
        board_id=board_id,
        receipt_id=receipt_id,
    )
    if receipt is None:
        raise RuntimeError("semantic_finding_receipt_missing")
    return await _receipt_currentness(semantic_port, receipt)


@dataclass(frozen=True, slots=True)
class ListSemanticGuidelineAssessmentsCommand:
    query: SemanticAssessmentListQuery

    def __post_init__(self) -> None:
        if not isinstance(self.query, SemanticAssessmentListQuery):
            raise ValueError("semantic_assessment_query_invalid")


@dataclass(frozen=True, slots=True)
class ListSemanticGuidelineAssessmentsResult:
    page: SemanticAssessmentPage


@dataclass(frozen=True, slots=True)
class GetSemanticGuidelineAssessmentCommand:
    board_id: str
    receipt_id: str
    projection: SemanticGuidelineProjection = SemanticGuidelineProjection.FULL

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "semantic_assessment_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "receipt_id",
            _bounded(
                self.receipt_id,
                POLICY_RECEIPT_ID_MAX_LENGTH,
                "semantic_assessment_receipt_id_required",
            ),
        )
        _profile(self.projection)


@dataclass(frozen=True, slots=True)
class GetSemanticGuidelineAssessmentResult:
    assessment: SemanticAssessmentProjection


@dataclass(frozen=True, slots=True)
class GetCurrentSemanticGuidelineAssessmentCommand:
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    binding_id: str
    projection: SemanticGuidelineProjection = SemanticGuidelineProjection.FULL

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "semantic_assessment_board_id_required",
            ),
        )
        if not isinstance(self.entity_type, PolicyEntityType):
            raise ValueError("semantic_assessment_entity_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _bounded(
                self.subject_id,
                POLICY_SUBJECT_ID_MAX_LENGTH,
                "semantic_assessment_subject_id_required",
            ),
        )
        object.__setattr__(
            self,
            "binding_id",
            _bounded(
                self.binding_id,
                GUIDELINE_BINDING_ID_MAX_LENGTH,
                "semantic_assessment_binding_id_required",
            ),
        )
        _profile(self.projection)


@dataclass(frozen=True, slots=True)
class GetCurrentSemanticGuidelineAssessmentResult:
    assessment: SemanticAssessmentProjection


@dataclass(frozen=True, slots=True)
class ListSemanticGuidelineFindingsCommand:
    query: SemanticFindingListQuery

    def __post_init__(self) -> None:
        if not isinstance(self.query, SemanticFindingListQuery):
            raise ValueError("semantic_finding_query_invalid")


@dataclass(frozen=True, slots=True)
class ListSemanticGuidelineFindingsResult:
    page: SemanticFindingPage


@dataclass(frozen=True, slots=True)
class ListSemanticMetricWaiversCommand:
    query: SemanticWaiverListQuery

    def __post_init__(self) -> None:
        if not isinstance(self.query, SemanticWaiverListQuery):
            raise ValueError("semantic_waiver_query_invalid")


@dataclass(frozen=True, slots=True)
class ListSemanticMetricWaiversResult:
    page: SemanticWaiverPage


def _semantic_waiver_identity(
    board_id: object,
    waiver_id: object,
) -> tuple[str, str]:
    return (
        _bounded(
            board_id,
            POLICY_BOARD_ID_MAX_LENGTH,
            "semantic_waiver_board_id_required",
        ),
        _bounded(
            waiver_id,
            POLICY_RECEIPT_ID_MAX_LENGTH,
            "semantic_waiver_id_required",
        ),
    )


@dataclass(frozen=True, slots=True)
class GetSemanticMetricWaiverCommand:
    board_id: str
    waiver_id: str
    evaluated_at: datetime
    projection: SemanticGuidelineProjection = SemanticGuidelineProjection.FULL

    def __post_init__(self) -> None:
        board_id, waiver_id = _semantic_waiver_identity(
            self.board_id,
            self.waiver_id,
        )
        if self.evaluated_at is None:
            raise ValueError("semantic_waiver_evaluated_at_required")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "waiver_id", waiver_id)
        object.__setattr__(
            self,
            "evaluated_at",
            _aware_utc(
                self.evaluated_at,
                _utc_now,
                "semantic_waiver_evaluated_at_invalid",
            ),
        )
        _profile(self.projection)


@dataclass(frozen=True, slots=True)
class GetSemanticMetricWaiverResult:
    waiver: SemanticWaiverProjection


@dataclass(frozen=True, slots=True)
class ListSemanticMetricWaiverEventsCommand:
    board_id: str
    waiver_id: str

    def __post_init__(self) -> None:
        board_id, waiver_id = _semantic_waiver_identity(
            self.board_id,
            self.waiver_id,
        )
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "waiver_id", waiver_id)


@dataclass(frozen=True, slots=True)
class ListSemanticMetricWaiverEventsResult:
    events: tuple[SemanticMetricWaiverEvent, ...]


@dataclass(frozen=True, slots=True)
class RequestSemanticMetricWaiverCommand:
    board_id: str
    metric_result_id: str
    finding_id: str
    receipt_id: str
    justification: str
    evidence_refs: tuple[EvidenceRef, ...]
    expires_at: datetime | None
    idempotency_key: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "semantic_waiver_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "metric_result_id",
            _bounded(
                self.metric_result_id,
                POLICY_RECEIPT_ID_MAX_LENGTH,
                "semantic_waiver_metric_result_id_required",
            ),
        )
        object.__setattr__(
            self,
            "finding_id",
            _bounded(
                self.finding_id,
                POLICY_FINDING_ID_MAX_LENGTH,
                "semantic_waiver_finding_id_required",
            ),
        )
        object.__setattr__(
            self,
            "receipt_id",
            _bounded(
                self.receipt_id,
                POLICY_RECEIPT_ID_MAX_LENGTH,
                "semantic_waiver_receipt_id_required",
            ),
        )
        if not isinstance(self.justification, str) or not self.justification.strip():
            raise ValueError("semantic_waiver_justification_required")
        object.__setattr__(self, "justification", self.justification.strip())
        object.__setattr__(self, "evidence_refs", _evidence(self.evidence_refs))
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "semantic_waiver_idempotency_key_required",
            ),
        )


@dataclass(frozen=True, slots=True)
class ReviewSemanticMetricWaiverCommand:
    board_id: str
    waiver_id: str
    decision: SemanticMetricWaiverEventType
    reason: str
    evidence_refs: tuple[EvidenceRef, ...]
    expected_waiver_revision: int
    idempotency_key: str

    def __post_init__(self) -> None:
        board_id, waiver_id = _semantic_waiver_identity(
            self.board_id,
            self.waiver_id,
        )
        if self.decision not in {
            SemanticMetricWaiverEventType.APPROVE,
            SemanticMetricWaiverEventType.REJECT,
        }:
            raise ValueError("semantic_waiver_review_decision_invalid")
        if (
            not isinstance(self.expected_waiver_revision, int)
            or isinstance(self.expected_waiver_revision, bool)
            or self.expected_waiver_revision < 1
        ):
            raise ValueError("semantic_waiver_revision_invalid")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("semantic_waiver_reason_required")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "waiver_id", waiver_id)
        object.__setattr__(self, "reason", self.reason.strip())
        object.__setattr__(self, "evidence_refs", _evidence(self.evidence_refs))
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "semantic_waiver_idempotency_key_required",
            ),
        )


@dataclass(frozen=True, slots=True)
class RevokeSemanticMetricWaiverCommand:
    board_id: str
    waiver_id: str
    reason: str
    evidence_refs: tuple[EvidenceRef, ...]
    expected_waiver_revision: int
    idempotency_key: str

    def __post_init__(self) -> None:
        board_id, waiver_id = _semantic_waiver_identity(
            self.board_id,
            self.waiver_id,
        )
        if (
            not isinstance(self.expected_waiver_revision, int)
            or isinstance(self.expected_waiver_revision, bool)
            or self.expected_waiver_revision < 1
        ):
            raise ValueError("semantic_waiver_revision_invalid")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("semantic_waiver_reason_required")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "waiver_id", waiver_id)
        object.__setattr__(self, "reason", self.reason.strip())
        object.__setattr__(self, "evidence_refs", _evidence(self.evidence_refs))
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "semantic_waiver_idempotency_key_required",
            ),
        )


@dataclass(frozen=True, slots=True)
class RevalidateSemanticMetricWaiverCommand:
    board_id: str
    waiver_id: str
    expected_waiver_revision: int
    evaluated_at: datetime
    idempotency_key: str

    def __post_init__(self) -> None:
        board_id, waiver_id = _semantic_waiver_identity(
            self.board_id,
            self.waiver_id,
        )
        if (
            not isinstance(self.expected_waiver_revision, int)
            or isinstance(self.expected_waiver_revision, bool)
            or self.expected_waiver_revision < 1
        ):
            raise ValueError("semantic_waiver_revision_invalid")
        if self.evaluated_at is None:
            raise ValueError("semantic_waiver_revalidation_evaluated_at_required")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "waiver_id", waiver_id)
        object.__setattr__(
            self,
            "evaluated_at",
            _aware_utc(
                self.evaluated_at,
                _utc_now,
                "semantic_waiver_revalidation_evaluated_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "semantic_waiver_idempotency_key_required",
            ),
        )


@dataclass(frozen=True, slots=True)
class SemanticMetricWaiverMutationResult:
    mutation: SemanticMetricWaiverMutation
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class RevalidateSemanticMetricWaiverResult:
    waiver_id: str
    waiver_revision: int
    status: SemanticMetricWaiverRevalidationStatus
    current: bool
    reason_code: SemanticMetricWaiverRevalidationReason
    replayed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "waiver_id",
            _bounded(
                self.waiver_id,
                POLICY_RECEIPT_ID_MAX_LENGTH,
                "semantic_waiver_id_required",
            ),
        )
        if (
            not isinstance(self.waiver_revision, int)
            or isinstance(self.waiver_revision, bool)
            or self.waiver_revision < 1
        ):
            raise ValueError("semantic_waiver_revision_invalid")
        if not isinstance(
            self.status,
            SemanticMetricWaiverRevalidationStatus,
        ):
            raise ValueError("semantic_waiver_revalidation_status_invalid")
        if not isinstance(self.current, bool):
            raise ValueError("semantic_waiver_revalidation_current_invalid")
        if not isinstance(
            self.reason_code,
            SemanticMetricWaiverRevalidationReason,
        ):
            raise ValueError("semantic_waiver_revalidation_reason_invalid")
        if self.current != (
            self.status is SemanticMetricWaiverRevalidationStatus.APPROVED
        ):
            raise ValueError("semantic_waiver_revalidation_current_invalid")
        if not isinstance(self.replayed, bool):
            raise ValueError("semantic_waiver_revalidation_replayed_invalid")


@dataclass(frozen=True, slots=True)
class ListSemanticPolicySkipsCommand:
    query: SemanticSkipListQuery

    def __post_init__(self) -> None:
        if not isinstance(self.query, SemanticSkipListQuery):
            raise ValueError("semantic_skip_query_invalid")


@dataclass(frozen=True, slots=True)
class ListSemanticPolicySkipsResult:
    page: SemanticSkipPage


@dataclass(frozen=True, slots=True)
class GetSemanticPolicySkipCommand:
    board_id: str
    skip_id: str
    projection: SemanticGuidelineProjection = SemanticGuidelineProjection.FULL

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "semantic_skip_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "skip_id",
            _bounded(
                self.skip_id,
                POLICY_RECEIPT_ID_MAX_LENGTH,
                "semantic_skip_id_required",
            ),
        )
        _profile(self.projection)


@dataclass(frozen=True, slots=True)
class GetSemanticPolicySkipResult:
    skip: SemanticSkipProjection


@dataclass(frozen=True, slots=True)
class CreateSemanticPolicySkipCommand:
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    expected_subject_version: int
    binding_id: str
    reason: str
    idempotency_key: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "semantic_skip_board_id_required",
            ),
        )
        if not isinstance(self.entity_type, PolicyEntityType):
            raise ValueError("semantic_skip_entity_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _bounded(
                self.subject_id,
                POLICY_SUBJECT_ID_MAX_LENGTH,
                "semantic_skip_subject_id_required",
            ),
        )
        object.__setattr__(
            self,
            "binding_id",
            _bounded(
                self.binding_id,
                GUIDELINE_BINDING_ID_MAX_LENGTH,
                "semantic_skip_binding_id_required",
            ),
        )
        if (
            not isinstance(self.expected_subject_version, int)
            or isinstance(self.expected_subject_version, bool)
            or self.expected_subject_version < 1
        ):
            raise ValueError("semantic_skip_expected_subject_version_invalid")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("semantic_skip_reason_required")
        object.__setattr__(self, "reason", self.reason.strip())
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "semantic_skip_idempotency_key_required",
            ),
        )


@dataclass(frozen=True, slots=True)
class RevokeSemanticPolicySkipCommand:
    board_id: str
    skip_id: str
    expected_skip_revision: int
    reason: str
    idempotency_key: str

    def __post_init__(self) -> None:
        GetSemanticPolicySkipCommand(self.board_id, self.skip_id)
        if (
            not isinstance(self.expected_skip_revision, int)
            or isinstance(self.expected_skip_revision, bool)
            or self.expected_skip_revision < 1
        ):
            raise ValueError("semantic_skip_revision_invalid")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("semantic_skip_reason_required")
        object.__setattr__(self, "board_id", self.board_id.strip())
        object.__setattr__(self, "skip_id", self.skip_id.strip())
        object.__setattr__(self, "reason", self.reason.strip())
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "semantic_skip_idempotency_key_required",
            ),
        )


@dataclass(frozen=True, slots=True)
class SemanticPolicySkipMutationResult:
    mutation: SemanticPolicySkipMutation
    replayed: bool = False


class ListSemanticGuidelineAssessmentsUseCase:
    async def execute(
        self,
        command: ListSemanticGuidelineAssessmentsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSemanticGuidelineAssessmentsResult:
        query = command.query
        _require_capability(actor, ASSESSMENTS_READ)
        await _require_board(uow, query.board_id, actor, write=False)
        port = await _semantic_port(uow)
        after = (
            None
            if query.cursor is None
            else (query.cursor.recorded_at, query.cursor.item_id)
        )
        selected: list[tuple[Any, SemanticAssessmentCurrentness]] = []
        has_more = False
        while True:
            receipts, raw_next = await port.list_semantic_assessment_receipts(
                board_id=query.board_id,
                entity_type=query.entity_type,
                subject_id=query.subject_id,
                guideline_id=query.guideline_id,
                binding_id=query.binding_id,
                outcome=query.outcome,
                after=after,
                limit=(
                    SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX
                    if query.currentness is not None
                    else query.limit
                ),
            )
            for receipt in receipts:
                currentness = await _receipt_currentness(port, receipt)
                if (
                    query.currentness is None
                    or currentness.currentness is query.currentness
                ):
                    selected.append((receipt, currentness))
                    if len(selected) > query.limit:
                        has_more = True
                        break
            if has_more:
                break
            if raw_next is None:
                break
            if query.currentness is None:
                has_more = True
                break
            if raw_next == after:
                raise RuntimeError("semantic_assessment_cursor_no_progress")
            after = raw_next
        page_values = selected[: query.limit]
        next_cursor = None
        if has_more and page_values:
            anchor = page_values[-1][0]
            next_cursor = SemanticAssessmentPageCursor(
                at=anchor.recorded_at,
                item_id=anchor.receipt_id,
                filter_digest=query.filter_digest,
                projection_digest=query.projection_digest,
            )
        return ListSemanticGuidelineAssessmentsResult(
            SemanticAssessmentPage(
                items=tuple(
                    project_semantic_assessment(
                        receipt,
                        currentness=currentness,
                        projection=query.projection,
                    )
                    for receipt, currentness in page_values
                ),
                limit=query.limit,
                next_cursor=next_cursor,
                has_more=has_more,
                projection=query.projection,
            )
        )


class GetSemanticGuidelineAssessmentUseCase:
    async def execute(
        self,
        command: GetSemanticGuidelineAssessmentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetSemanticGuidelineAssessmentResult:
        _require_capability(actor, ASSESSMENTS_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = await _semantic_port(uow)
        receipt = await port.get_semantic_assessment_receipt(
            board_id=command.board_id,
            receipt_id=command.receipt_id,
        )
        if receipt is None:
            raise EntityNotFoundError(
                "semantic_guideline_assessment",
                command.receipt_id,
            )
        currentness = await _receipt_currentness(port, receipt)
        return GetSemanticGuidelineAssessmentResult(
            project_semantic_assessment(
                receipt,
                currentness=currentness,
                projection=command.projection,
            )
        )


class GetCurrentSemanticGuidelineAssessmentUseCase:
    async def execute(
        self,
        command: GetCurrentSemanticGuidelineAssessmentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetCurrentSemanticGuidelineAssessmentResult:
        _require_capability(actor, ASSESSMENTS_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = await _semantic_port(uow)
        receipt = await port.get_current_semantic_assessment_receipt(
            board_id=command.board_id,
            entity_type=command.entity_type,
            subject_id=command.subject_id,
            binding_id=command.binding_id,
        )
        if receipt is None:
            raise EntityNotFoundError(
                "current_semantic_guideline_assessment",
                f"{command.subject_id}:{command.binding_id}",
            )
        currentness = await _receipt_currentness(port, receipt)
        if currentness.currentness is not PolicyCurrentness.CURRENT:
            raise RuntimeError("semantic_current_assessment_adapter_stale")
        return GetCurrentSemanticGuidelineAssessmentResult(
            project_semantic_assessment(
                receipt,
                currentness=currentness,
                projection=command.projection,
            )
        )


class ListSemanticGuidelineFindingsUseCase:
    async def execute(
        self,
        command: ListSemanticGuidelineFindingsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSemanticGuidelineFindingsResult:
        query = command.query
        _require_capability(actor, ASSESSMENTS_READ)
        await _require_board(uow, query.board_id, actor, write=False)
        port = await _semantic_port(uow)
        findings, raw_next = await port.list_semantic_guideline_findings(
            board_id=query.board_id,
            entity_type=query.entity_type,
            subject_id=query.subject_id,
            receipt_id=query.receipt_id,
            guideline_id=query.guideline_id,
            binding_id=query.binding_id,
            metric_id=query.metric_id,
            outcome=query.outcome,
            after=(
                None
                if query.cursor is None
                else (query.cursor.created_at, query.cursor.item_id)
            ),
            limit=query.limit,
        )
        items: list[SemanticFindingProjection] = []
        for finding in findings:
            items.append(
                project_semantic_finding(
                    finding,
                    currentness=await _finding_currentness(
                        port,
                        board_id=query.board_id,
                        receipt_id=finding.receipt_id,
                    ),
                    projection=query.projection,
                )
            )
        next_cursor = (
            None
            if raw_next is None or not findings
            else SemanticFindingPageCursor(
                at=findings[-1].created_at,
                item_id=findings[-1].finding_id,
                filter_digest=query.filter_digest,
                projection_digest=query.projection_digest,
            )
        )
        return ListSemanticGuidelineFindingsResult(
            SemanticFindingPage(
                items=tuple(items),
                limit=query.limit,
                next_cursor=next_cursor,
                has_more=next_cursor is not None,
                projection=query.projection,
            )
        )


class ListSemanticMetricWaiversUseCase:
    async def execute(
        self,
        command: ListSemanticMetricWaiversCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSemanticMetricWaiversResult:
        query = command.query
        _require_capability(actor, WAIVER_READ)
        await _require_board(uow, query.board_id, actor, write=False)
        port = await _semantic_port(uow)
        after = (
            None
            if query.cursor is None
            else (query.cursor.requested_at, query.cursor.item_id)
        )
        selected: list[tuple[SemanticMetricWaiver, SemanticWaiverProjection]] = []
        has_more = False
        while True:
            waivers, raw_next = await port.list_board_semantic_waivers(
                board_id=query.board_id,
                evaluated_at=query.evaluated_at,
                finding_id=query.finding_id,
                metric_result_id=query.metric_result_id,
                receipt_id=query.receipt_id,
                guideline_id=query.guideline_id,
                binding_id=query.binding_id,
                metric_id=query.metric_id,
                entity_type=query.entity_type,
                subject_id=query.subject_id,
                # Lifecycle status cannot implement the authoritative as-of
                # filter: an unmutated approved head may be effectively
                # expired at query.evaluated_at.
                status=None,
                after=after,
                limit=(
                    SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX
                    if query.status is not None
                    else query.limit
                ),
            )
            for waiver in waivers:
                item = project_semantic_waiver(
                    waiver,
                    currentness=await _finding_currentness(
                        port,
                        board_id=query.board_id,
                        receipt_id=waiver.anchor.receipt_id,
                    ),
                    projection=query.projection,
                    evaluated_at=query.evaluated_at,
                )
                if query.status is None or item.status is query.status:
                    selected.append((waiver, item))
                    if len(selected) > query.limit:
                        has_more = True
                        break
            if has_more:
                break
            if raw_next is None:
                break
            if query.status is None:
                has_more = True
                break
            if raw_next == after:
                raise RuntimeError("semantic_waiver_cursor_no_progress")
            after = raw_next
        page_values = selected[: query.limit]
        next_cursor = None
        if has_more and page_values:
            anchor = page_values[-1][0]
            next_cursor = SemanticWaiverPageCursor(
                at=anchor.requested_at,
                item_id=anchor.waiver_id,
                filter_digest=query.filter_digest,
                projection_digest=query.projection_digest,
            )
        return ListSemanticMetricWaiversResult(
            SemanticWaiverPage(
                items=tuple(item for _, item in page_values),
                limit=query.limit,
                next_cursor=next_cursor,
                has_more=has_more,
                projection=query.projection,
            )
        )


class GetSemanticMetricWaiverUseCase:
    async def execute(
        self,
        command: GetSemanticMetricWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetSemanticMetricWaiverResult:
        _require_capability(actor, WAIVER_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = await _semantic_port(uow)
        waiver = await port.get_semantic_waiver(
            board_id=command.board_id,
            waiver_id=command.waiver_id,
        )
        if waiver is None:
            raise EntityNotFoundError("semantic_metric_waiver", command.waiver_id)
        currentness = await _finding_currentness(
            port,
            board_id=command.board_id,
            receipt_id=waiver.anchor.receipt_id,
        )
        return GetSemanticMetricWaiverResult(
            project_semantic_waiver(
                waiver,
                currentness=currentness,
                projection=command.projection,
                evaluated_at=command.evaluated_at,
            )
        )


class ListSemanticMetricWaiverEventsUseCase:
    async def execute(
        self,
        command: ListSemanticMetricWaiverEventsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSemanticMetricWaiverEventsResult:
        _require_capability(actor, WAIVER_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = await _semantic_port(uow)
        waiver = await port.get_semantic_waiver(
            board_id=command.board_id,
            waiver_id=command.waiver_id,
        )
        if waiver is None:
            raise EntityNotFoundError("semantic_metric_waiver", command.waiver_id)
        events, next_cursor = await port.list_semantic_waiver_events(
            board_id=command.board_id,
            waiver_id=command.waiver_id,
            limit=SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX,
        )
        if next_cursor is not None:
            raise RuntimeError("semantic_waiver_event_history_too_large")
        return ListSemanticMetricWaiverEventsResult(events)


def _waiver_replay_matches(
    replay: SemanticMetricWaiverMutation,
    *,
    event_type: SemanticMetricWaiverEventType,
    actor_id: str,
    reason: str,
    evidence_refs: tuple[EvidenceRef, ...],
    expires_at: datetime | None,
    waiver_id: str | None = None,
    expected_waiver_revision: int | None = None,
    metric_result_id: str | None = None,
    finding_id: str | None = None,
    receipt_id: str | None = None,
) -> bool:
    event = replay.event
    return (
        event.event_type is event_type
        and event.actor_id == actor_id
        and (waiver_id is None or event.waiver_id == waiver_id)
        and (
            expected_waiver_revision is None
            or event.waiver_revision == expected_waiver_revision + 1
        )
        and event.reason == reason
        and event.evidence_refs == evidence_refs
        and (
            (
                event_type is SemanticMetricWaiverEventType.REQUEST
                and event.expires_at == expires_at
            )
            or (
                event_type
                in {
                    SemanticMetricWaiverEventType.APPROVE,
                }
                and (expires_at is None or event.expires_at == expires_at)
            )
            or event_type
            in {
                SemanticMetricWaiverEventType.REJECT,
                SemanticMetricWaiverEventType.REVOKE,
            }
        )
        and (finding_id is None or replay.waiver.anchor.finding_id == finding_id)
        and (
            metric_result_id is None
            or replay.waiver.anchor.metric_result_id == metric_result_id
        )
        and (receipt_id is None or replay.waiver.anchor.receipt_id == receipt_id)
    )


class RequestSemanticMetricWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: RequestSemanticMetricWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SemanticMetricWaiverMutationResult:
        _require_capability(actor, WAIVER_REQUEST)
        await _require_board(uow, command.board_id, actor, write=True)
        port = await _semantic_port(uow)
        replay = await port.get_semantic_waiver_by_idempotency(
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
        )
        if replay is not None:
            if not _waiver_replay_matches(
                replay,
                event_type=SemanticMetricWaiverEventType.REQUEST,
                actor_id=actor.actor_id,
                reason=command.justification,
                evidence_refs=command.evidence_refs,
                expires_at=command.expires_at,
                metric_result_id=command.metric_result_id,
                finding_id=command.finding_id,
                receipt_id=command.receipt_id,
            ):
                raise GuidelinePolicyIdempotencyConflict(
                    "semantic_waiver_idempotency_conflict"
                )
            return SemanticMetricWaiverMutationResult(replay, replayed=True)
        finding = await port.get_semantic_guideline_finding(
            board_id=command.board_id,
            finding_id=command.finding_id,
        )
        if finding is None:
            raise EntityNotFoundError(
                "semantic_guideline_finding",
                command.finding_id,
            )
        if (
            finding.metric_result_id != command.metric_result_id
            or finding.receipt_id != command.receipt_id
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_anchor_identity_mismatch"
            )
        metric_result = await port.get_semantic_metric_result(
            board_id=command.board_id,
            metric_result_id=command.metric_result_id,
        )
        if metric_result is None:
            raise EntityNotFoundError(
                "semantic_metric_result",
                command.metric_result_id,
            )
        receipt = await port.get_semantic_assessment_receipt(
            board_id=command.board_id,
            receipt_id=command.receipt_id,
        )
        if receipt is None:
            raise EntityNotFoundError(
                "semantic_guideline_assessment",
                command.receipt_id,
            )
        if (
            metric_result.receipt_id != command.receipt_id
            or metric_result.subject != finding.subject
            or metric_result.binding_id != finding.binding_id
            or metric_result.guideline_id != finding.guideline_id
            or metric_result.revision_id != finding.guideline_revision_id
            or metric_result.metric_id != finding.metric_id
            or metric_result.metric_code != finding.metric_code
            or (
                semantic_metric_result_digest_v1(metric_result)
                != finding.metric_result_digest
            )
            or receipt.receipt_id != command.receipt_id
            or receipt.receipt_digest != finding.receipt_digest
            or receipt.subject != finding.subject
            or receipt.binding_id != finding.binding_id
            or receipt.guideline_id != finding.guideline_id
            or receipt.guideline_revision_id != finding.guideline_revision_id
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_anchor_integrity_mismatch"
            )
        if not (await _receipt_currentness(port, receipt)).is_current:
            raise SemanticAssessmentContractError("semantic_waiver_anchor_stale")
        occurred_at = _aware_utc(
            None,
            self._clock,
            "semantic_waiver_requested_at_invalid",
        )
        mutation = request_semantic_metric_waiver(
            waiver_id=self._id_factory(
                "semantic-metric-waiver",
                f"{command.board_id}:{command.idempotency_key}",
            ),
            event_id=self._id_factory(
                "semantic-metric-waiver-event",
                f"{command.board_id}:{command.idempotency_key}",
            ),
            anchor=SemanticMetricWaiverAnchor.from_finding(
                finding,
                assessment_assessor_id=receipt.assessor.agent_id,
            ),
            justification=command.justification,
            evidence_refs=command.evidence_refs,
            requested_by=actor.actor_id,
            requested_at=occurred_at,
            expires_at=command.expires_at,
            idempotency_key=command.idempotency_key,
        )

        async def mutate() -> SemanticMetricWaiverMutation:
            return await port.save_semantic_metric_waiver_mutation(mutation=mutation)

        return SemanticMetricWaiverMutationResult(await _write(uow, mutate))


async def _load_semantic_waiver(
    port: SemanticGuidelineAssessmentPersistencePort,
    *,
    board_id: str,
    waiver_id: str,
) -> SemanticMetricWaiver:
    waiver = await port.get_semantic_waiver(
        board_id=board_id,
        waiver_id=waiver_id,
    )
    if waiver is None:
        raise EntityNotFoundError("semantic_metric_waiver", waiver_id)
    return waiver


async def _transition_waiver(
    *,
    command: ReviewSemanticMetricWaiverCommand | RevokeSemanticMetricWaiverCommand,
    actor: ActorContext,
    uow: PulseUnitOfWork,
    capability: str,
    event_type: SemanticMetricWaiverEventType,
    clock: Clock,
    id_factory: IdFactory,
) -> SemanticMetricWaiverMutationResult:
    _require_capability(actor, capability)
    await _require_board(uow, command.board_id, actor, write=True)
    port = await _semantic_port(uow)
    replay = await port.get_semantic_waiver_by_idempotency(
        board_id=command.board_id,
        idempotency_key=command.idempotency_key,
    )
    if replay is not None:
        if not _waiver_replay_matches(
            replay,
            event_type=event_type,
            actor_id=actor.actor_id,
            reason=command.reason,
            evidence_refs=command.evidence_refs,
            expires_at=None,
            waiver_id=command.waiver_id,
            expected_waiver_revision=command.expected_waiver_revision,
        ):
            raise GuidelinePolicyIdempotencyConflict(
                "semantic_waiver_idempotency_conflict"
            )
        return SemanticMetricWaiverMutationResult(replay, replayed=True)
    current = await _load_semantic_waiver(
        port,
        board_id=command.board_id,
        waiver_id=command.waiver_id,
    )
    if event_type in {
        SemanticMetricWaiverEventType.APPROVE,
        SemanticMetricWaiverEventType.REJECT,
    }:
        if actor.actor_id in {
            current.requested_by,
            current.anchor.assessment_assessor_id,
        }:
            raise SemanticAssessmentContractError(
                "semantic_waiver_independent_review_required"
            )
    mutation = transition_semantic_metric_waiver(
        current,
        event_id=id_factory(
            "semantic-metric-waiver-event",
            f"{command.board_id}:{command.idempotency_key}",
        ),
        expected_waiver_revision=command.expected_waiver_revision,
        event_type=event_type,
        actor_id=actor.actor_id,
        occurred_at=_aware_utc(
            None,
            clock,
            "semantic_waiver_event_occurred_at_invalid",
        ),
        reason=command.reason,
        evidence_refs=command.evidence_refs,
        idempotency_key=command.idempotency_key,
        expires_at=None,
    )

    async def mutate() -> SemanticMetricWaiverMutation:
        return await port.save_semantic_metric_waiver_mutation(mutation=mutation)

    return SemanticMetricWaiverMutationResult(await _write(uow, mutate))


class ReviewSemanticMetricWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: ReviewSemanticMetricWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SemanticMetricWaiverMutationResult:
        return await _transition_waiver(
            command=command,
            actor=actor,
            uow=uow,
            capability=WAIVER_REVIEW,
            event_type=command.decision,
            clock=self._clock,
            id_factory=self._id_factory,
        )


class RevokeSemanticMetricWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: RevokeSemanticMetricWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SemanticMetricWaiverMutationResult:
        return await _transition_waiver(
            command=command,
            actor=actor,
            uow=uow,
            capability=WAIVER_REVOKE,
            event_type=SemanticMetricWaiverEventType.REVOKE,
            clock=self._clock,
            id_factory=self._id_factory,
        )


_REVALIDATION_REASON_BY_CURRENTNESS = {
    SemanticAssessmentCurrentnessReason.CURRENT_SNAPSHOT_MISSING: (
        SemanticMetricWaiverRevalidationReason.ANCHOR_MISSING
    ),
    SemanticAssessmentCurrentnessReason.SUBJECT_VERSION_CHANGED: (
        SemanticMetricWaiverRevalidationReason.SUBJECT_SCOPE_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.SUBJECT_CONTENT_CHANGED: (
        SemanticMetricWaiverRevalidationReason.SUBJECT_SCOPE_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.GUIDELINE_REVISION_CHANGED: (
        SemanticMetricWaiverRevalidationReason.GUIDELINE_REVISION_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.GUIDELINE_REVISION_DIGEST_CHANGED: (
        SemanticMetricWaiverRevalidationReason.GUIDELINE_REVISION_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.BINDING_REVISION_CHANGED: (
        SemanticMetricWaiverRevalidationReason.BINDING_CONFIGURATION_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.BINDING_CONFIGURATION_CHANGED: (
        SemanticMetricWaiverRevalidationReason.BINDING_CONFIGURATION_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.POLICY_SET_CHANGED: (
        SemanticMetricWaiverRevalidationReason.BINDING_CONFIGURATION_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.BINDING_HEAD_CHANGED: (
        SemanticMetricWaiverRevalidationReason.BINDING_CONFIGURATION_CHANGED
    ),
    SemanticAssessmentCurrentnessReason.INPUT_DIGEST_CHANGED: (
        SemanticMetricWaiverRevalidationReason.SUBJECT_SCOPE_CHANGED
    ),
}

_REVALIDATION_REASON_BY_EXPIRY = {
    SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY: (
        SemanticMetricWaiverRevalidationReason.SCHEDULED_EXPIRY
    ),
    SemanticMetricWaiverExpireReason.SUBJECT_SCOPE_CHANGED: (
        SemanticMetricWaiverRevalidationReason.SUBJECT_SCOPE_CHANGED
    ),
    SemanticMetricWaiverExpireReason.GUIDELINE_REVISION_CHANGED: (
        SemanticMetricWaiverRevalidationReason.GUIDELINE_REVISION_CHANGED
    ),
    SemanticMetricWaiverExpireReason.BINDING_CONFIGURATION_CHANGED: (
        SemanticMetricWaiverRevalidationReason.BINDING_CONFIGURATION_CHANGED
    ),
    SemanticMetricWaiverExpireReason.METRIC_RESULT_CHANGED: (
        SemanticMetricWaiverRevalidationReason.METRIC_RESULT_CHANGED
    ),
}


def _revalidation_reason_from_currentness(
    currentness: SemanticAssessmentCurrentness,
) -> SemanticMetricWaiverRevalidationReason:
    for reason in currentness.reasons:
        return _REVALIDATION_REASON_BY_CURRENTNESS[reason]
    return SemanticMetricWaiverRevalidationReason.CURRENT


def _merged_evidence_refs(
    *groups: tuple[EvidenceRef, ...],
) -> tuple[EvidenceRef, ...]:
    unique = {
        (
            item.source_type,
            item.source_id,
            item.source_version,
            item.content_hash,
        ): item
        for group in groups
        for item in group
    }
    return tuple(unique[key] for key in sorted(unique))


async def _evaluate_semantic_waiver_revalidation(
    *,
    port: SemanticGuidelineAssessmentPersistencePort,
    waiver: SemanticMetricWaiver,
    evaluated_at: datetime,
) -> tuple[
    SemanticMetricWaiverRevalidationStatus,
    SemanticMetricWaiverRevalidationReason,
    tuple[SemanticAssessmentCurrentnessReason, ...],
    bool,
    tuple[EvidenceRef, ...],
]:
    anchor = waiver.anchor
    receipt = await port.get_semantic_assessment_receipt(
        board_id=anchor.subject.board_id,
        receipt_id=anchor.receipt_id,
    )
    finding = await port.get_semantic_guideline_finding(
        board_id=anchor.subject.board_id,
        finding_id=anchor.finding_id,
    )
    metric_result = await port.get_semantic_metric_result(
        board_id=anchor.subject.board_id,
        metric_result_id=anchor.metric_result_id,
    )
    expiry_observed = bool(
        waiver.expires_at is not None and waiver.expires_at <= evaluated_at
    )
    evidence = _merged_evidence_refs(
        waiver.evidence_refs,
        finding.evidence_refs if finding is not None else (),
        metric_result.evidence_refs if metric_result is not None else (),
    )

    stale_reason: SemanticMetricWaiverRevalidationReason | None = None
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...] = ()
    if receipt is None or finding is None or metric_result is None:
        stale_reason = SemanticMetricWaiverRevalidationReason.ANCHOR_MISSING
    elif (
        receipt.receipt_digest != anchor.receipt_digest
        or receipt.assessor.agent_id != anchor.assessment_assessor_id
    ):
        stale_reason = SemanticMetricWaiverRevalidationReason.ANCHOR_MISSING
    elif (
        not anchor.matches_finding(finding)
        or metric_result.metric_result_id != anchor.metric_result_id
        or metric_result.receipt_id != anchor.receipt_id
        or metric_result.subject != anchor.subject
        or metric_result.binding_id != anchor.binding_id
        or metric_result.guideline_id != anchor.guideline_id
        or metric_result.revision_id != anchor.guideline_revision_id
        or metric_result.metric_id != anchor.metric_id
        or metric_result.metric_code != anchor.metric_code
        or (
            semantic_metric_result_digest_v1(metric_result)
            != anchor.metric_result_digest
        )
    ):
        stale_reason = SemanticMetricWaiverRevalidationReason.METRIC_RESULT_CHANGED
    else:
        snapshot = await port.resolve_semantic_assessment_current_snapshot(
            board_id=anchor.subject.board_id,
            entity_type=anchor.subject.entity_type,
            subject_id=anchor.subject.subject_id,
            binding_id=anchor.binding_id,
            lock=True,
        )
        try:
            currentness = assess_semantic_assessment_currentness(
                receipt,
                snapshot,
            )
        except SemanticAssessmentContractError as exc:
            if str(exc) != "semantic_currentness_binding_scope_mismatch":
                raise
            currentness = SemanticAssessmentCurrentness(
                receipt_id=receipt.receipt_id,
                currentness=PolicyCurrentness.STALE,
                reasons=(
                    SemanticAssessmentCurrentnessReason.BINDING_CONFIGURATION_CHANGED,
                ),
            )
        currentness_reasons = currentness.reasons
        if not currentness.is_current:
            stale_reason = _revalidation_reason_from_currentness(currentness)

    if waiver.status is SemanticMetricWaiverStatus.REVOKED:
        return (
            SemanticMetricWaiverRevalidationStatus.REVOKED,
            SemanticMetricWaiverRevalidationReason.REVOKED,
            currentness_reasons,
            expiry_observed,
            evidence,
        )
    if stale_reason is not None:
        return (
            SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE,
            stale_reason,
            currentness_reasons,
            expiry_observed,
            evidence,
        )
    if waiver.status is SemanticMetricWaiverStatus.EXPIRED:
        if waiver.expire_reason is None:
            raise SemanticAssessmentContractError(
                "semantic_waiver_expire_reason_required"
            )
        return (
            SemanticMetricWaiverRevalidationStatus.EXPIRED,
            _REVALIDATION_REASON_BY_EXPIRY[waiver.expire_reason],
            (),
            expiry_observed,
            evidence,
        )
    if expiry_observed:
        return (
            SemanticMetricWaiverRevalidationStatus.EXPIRED,
            SemanticMetricWaiverRevalidationReason.SCHEDULED_EXPIRY,
            (),
            True,
            evidence,
        )
    return (
        SemanticMetricWaiverRevalidationStatus.APPROVED,
        SemanticMetricWaiverRevalidationReason.CURRENT,
        (),
        False,
        evidence,
    )


def _revalidation_result(
    mutation: SemanticMetricWaiverMutation,
    *,
    replayed: bool,
) -> RevalidateSemanticMetricWaiverResult:
    event = mutation.event
    if (
        event.event_type is not SemanticMetricWaiverEventType.REVALIDATE
        or event.revalidation_status is None
        or event.revalidation_current is None
        or event.revalidation_reason_code is None
    ):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_event_invalid"
        )
    return RevalidateSemanticMetricWaiverResult(
        waiver_id=event.waiver_id,
        waiver_revision=event.waiver_revision,
        status=event.revalidation_status,
        current=event.revalidation_current,
        reason_code=event.revalidation_reason_code,
        replayed=replayed,
    )


class RevalidateSemanticMetricWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: RevalidateSemanticMetricWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> RevalidateSemanticMetricWaiverResult:
        _require_capability(actor, WAIVER_REVALIDATE)
        await _require_board(uow, command.board_id, actor, write=True)
        port = await _semantic_port(uow)
        replay = await port.get_semantic_waiver_by_idempotency(
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
        )
        if replay is not None:
            event = replay.event
            if not (
                event.event_type is SemanticMetricWaiverEventType.REVALIDATE
                and event.waiver_id == command.waiver_id
                and event.actor_id == actor.actor_id
                and event.waiver_revision == command.expected_waiver_revision + 1
                and event.evaluated_at == command.evaluated_at
            ):
                raise GuidelinePolicyIdempotencyConflict(
                    "semantic_waiver_idempotency_conflict"
                )
            return _revalidation_result(replay, replayed=True)
        current = await _load_semantic_waiver(
            port,
            board_id=command.board_id,
            waiver_id=command.waiver_id,
        )
        if actor.actor_id in {
            current.requested_by,
            current.anchor.assessment_assessor_id,
        }:
            raise SemanticAssessmentContractError(
                "semantic_waiver_independent_review_required"
            )
        (
            status,
            reason_code,
            currentness_reasons,
            expiry_observed,
            evidence_refs,
        ) = await _evaluate_semantic_waiver_revalidation(
            port=port,
            waiver=current,
            evaluated_at=command.evaluated_at,
        )
        mutation = revalidate_semantic_metric_waiver(
            current,
            event_id=self._id_factory(
                "semantic-metric-waiver-event",
                f"{command.board_id}:{command.idempotency_key}",
            ),
            expected_waiver_revision=(command.expected_waiver_revision),
            actor_id=actor.actor_id,
            occurred_at=_aware_utc(
                None,
                self._clock,
                "semantic_waiver_event_occurred_at_invalid",
            ),
            evaluated_at=command.evaluated_at,
            status=status,
            reason_code=reason_code,
            currentness_reasons=currentness_reasons,
            scheduled_expiry_observed=expiry_observed,
            evidence_refs=evidence_refs,
            idempotency_key=command.idempotency_key,
        )

        async def mutate() -> SemanticMetricWaiverMutation:
            return await port.save_semantic_metric_waiver_mutation(mutation=mutation)

        saved = await _write(uow, mutate)
        return _revalidation_result(saved, replayed=False)


def _require_human_rest(actor: ActorContext) -> None:
    if actor.source != "rest" or actor.actor_kind != "human":
        raise PermissionDeniedError("human_session_required")


async def _load_skip_authority(
    *,
    uow: PulseUnitOfWork,
    command: CreateSemanticPolicySkipCommand,
) -> tuple[PolicySubjectSnapshot, BoardGuidelineBinding, GuidelineRevision]:
    semantic_port = await _semantic_port(uow)
    subject = await semantic_port.resolve_policy_subject_snapshot(
        board_id=command.board_id,
        entity_type=command.entity_type,
        subject_id=command.subject_id,
        lock=True,
    )
    if subject is None:
        raise EntityNotFoundError("policy_subject", command.subject_id)
    if subject.subject.subject_version != command.expected_subject_version:
        raise SemanticAssessmentContractError("semantic_skip_subject_stale")
    policy_port = uow.services.guidelines.policy_persistence()
    bindings = tuple(
        binding
        for binding in await policy_port.list_bindings(board_id=command.board_id)
        if binding.state is GuidelineBindingState.ACTIVE
        and binding.binding_id == command.binding_id
    )
    if len(bindings) != 1:
        raise EntityNotFoundError("guideline_binding", command.binding_id)
    binding = bindings[0]
    revision = await policy_port.get_revision(
        guideline_id=binding.guideline_id,
        revision_id=binding.revision_id,
    )
    if revision is None:
        raise EntityNotFoundError(
            "guideline_revision",
            binding.revision_id,
        )
    return subject, binding, revision


class ListSemanticPolicySkipsUseCase:
    async def execute(
        self,
        command: ListSemanticPolicySkipsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSemanticPolicySkipsResult:
        _require_human_rest(actor)
        _require_capability(actor, ADOPTION_MANAGE)
        query = command.query
        await _require_board(uow, query.board_id, actor, write=False)
        port = await _semantic_port(uow)
        after = (
            None
            if query.cursor is None
            else (query.cursor.created_at, query.cursor.item_id)
        )
        selected: list[
            tuple[SemanticPolicySkip, SemanticAssessmentCurrentSnapshot | None]
        ] = []
        has_more = False
        while True:
            skips, raw_next = await port.list_semantic_policy_skips(
                board_id=query.board_id,
                entity_type=query.entity_type,
                subject_id=query.subject_id,
                binding_id=query.binding_id,
                status=query.status,
                after=after,
                limit=(
                    SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX
                    if query.currentness is not None
                    else query.limit
                ),
            )
            for skip in skips:
                current = await port.resolve_semantic_assessment_current_snapshot(
                    board_id=query.board_id,
                    entity_type=skip.scope.subject.entity_type,
                    subject_id=skip.scope.subject.subject_id,
                    binding_id=skip.scope.binding_id,
                    lock=False,
                )
                projected = project_semantic_skip(
                    skip,
                    current=current,
                    projection=SemanticGuidelineProjection.SUMMARY,
                )
                if (
                    query.currentness is None
                    or projected.currentness is query.currentness
                ):
                    selected.append((skip, current))
                    if len(selected) > query.limit:
                        has_more = True
                        break
            if has_more:
                break
            if raw_next is None:
                break
            if query.currentness is None:
                has_more = True
                break
            if raw_next == after:
                raise RuntimeError("semantic_skip_cursor_no_progress")
            after = raw_next
        page_values = selected[: query.limit]
        items: list[SemanticSkipProjection] = [
            project_semantic_skip(
                skip,
                current=current,
                projection=query.projection,
            )
            for skip, current in page_values
        ]
        next_cursor = (
            None
            if not has_more or not page_values
            else SemanticSkipPageCursor(
                at=page_values[-1][0].created_at,
                item_id=page_values[-1][0].skip_id,
                filter_digest=query.filter_digest,
                projection_digest=query.projection_digest,
            )
        )
        return ListSemanticPolicySkipsResult(
            SemanticSkipPage(
                items=tuple(items),
                limit=query.limit,
                next_cursor=next_cursor,
                has_more=has_more,
                projection=query.projection,
            )
        )


class GetSemanticPolicySkipUseCase:
    async def execute(
        self,
        command: GetSemanticPolicySkipCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetSemanticPolicySkipResult:
        _require_human_rest(actor)
        _require_capability(actor, ADOPTION_MANAGE)
        await _require_board(uow, command.board_id, actor, write=False)
        port = await _semantic_port(uow)
        skip = await port.get_semantic_skip(
            board_id=command.board_id,
            skip_id=command.skip_id,
        )
        if skip is None:
            raise EntityNotFoundError("semantic_policy_skip", command.skip_id)
        current = await port.resolve_semantic_assessment_current_snapshot(
            board_id=command.board_id,
            entity_type=skip.scope.subject.entity_type,
            subject_id=skip.scope.subject.subject_id,
            binding_id=skip.scope.binding_id,
            lock=False,
        )
        return GetSemanticPolicySkipResult(
            project_semantic_skip(
                skip,
                current=current,
                projection=command.projection,
            )
        )


class CreateSemanticPolicySkipUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: CreateSemanticPolicySkipCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SemanticPolicySkipMutationResult:
        _require_human_rest(actor)
        _require_capability(actor, ADOPTION_MANAGE)
        await _require_board(uow, command.board_id, actor, write=True)
        port = await _semantic_port(uow)
        subject, binding, revision = await _load_skip_authority(
            uow=uow,
            command=command,
        )
        scope = SemanticPolicySkipScope.from_authority(
            subject_snapshot=subject,
            binding=binding,
            revision=revision,
        )
        replay = await port.get_semantic_skip_event_by_idempotency(
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
        )
        if replay is not None:
            if (
                replay.event.event_type is not SemanticPolicySkipEventType.CREATE
                or replay.event.actor_id != actor.actor_id
                or replay.event.reason != command.reason
                or replay.skip.scope != scope
            ):
                raise GuidelinePolicyIdempotencyConflict(
                    "semantic_skip_idempotency_conflict"
                )
            return SemanticPolicySkipMutationResult(replay, replayed=True)
        mutation = create_semantic_policy_skip(
            skip_id=self._id_factory(
                "semantic-policy-skip",
                f"{command.board_id}:{command.idempotency_key}",
            ),
            event_id=self._id_factory(
                "semantic-policy-skip-event",
                f"{command.board_id}:{command.idempotency_key}",
            ),
            scope=scope,
            reason=command.reason,
            actor_id=actor.actor_id,
            actor_kind=SemanticExceptionActorKind.HUMAN,
            occurred_at=_aware_utc(
                None,
                self._clock,
                "semantic_skip_event_occurred_at_invalid",
            ),
            idempotency_key=command.idempotency_key,
        )

        async def mutate() -> SemanticPolicySkipMutation:
            return await port.save_semantic_policy_skip_mutation(mutation=mutation)

        return SemanticPolicySkipMutationResult(await _write(uow, mutate))


class RevokeSemanticPolicySkipUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: RevokeSemanticPolicySkipCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SemanticPolicySkipMutationResult:
        _require_human_rest(actor)
        _require_capability(actor, ADOPTION_MANAGE)
        await _require_board(uow, command.board_id, actor, write=True)
        port = await _semantic_port(uow)
        replay = await port.get_semantic_skip_event_by_idempotency(
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
        )
        if replay is not None:
            if (
                replay.event.event_type is not SemanticPolicySkipEventType.REVOKE
                or replay.event.actor_id != actor.actor_id
                or replay.event.reason != command.reason
                or replay.skip.skip_id != command.skip_id
            ):
                raise GuidelinePolicyIdempotencyConflict(
                    "semantic_skip_idempotency_conflict"
                )
            return SemanticPolicySkipMutationResult(replay, replayed=True)
        current = await port.get_semantic_skip(
            board_id=command.board_id,
            skip_id=command.skip_id,
        )
        if current is None:
            raise EntityNotFoundError("semantic_policy_skip", command.skip_id)
        mutation = revoke_semantic_policy_skip(
            current,
            event_id=self._id_factory(
                "semantic-policy-skip-event",
                f"{command.board_id}:{command.idempotency_key}",
            ),
            expected_skip_revision=command.expected_skip_revision,
            actor_id=actor.actor_id,
            actor_kind=SemanticExceptionActorKind.HUMAN,
            occurred_at=_aware_utc(
                None,
                self._clock,
                "semantic_skip_event_occurred_at_invalid",
            ),
            reason=command.reason,
            idempotency_key=command.idempotency_key,
        )

        async def mutate() -> SemanticPolicySkipMutation:
            return await port.save_semantic_policy_skip_mutation(mutation=mutation)

        return SemanticPolicySkipMutationResult(await _write(uow, mutate))


__all__ = [
    "CreateSemanticPolicySkipCommand",
    "CreateSemanticPolicySkipUseCase",
    "GetCurrentSemanticGuidelineAssessmentCommand",
    "GetCurrentSemanticGuidelineAssessmentResult",
    "GetCurrentSemanticGuidelineAssessmentUseCase",
    "GetSemanticGuidelineAssessmentCommand",
    "GetSemanticGuidelineAssessmentResult",
    "GetSemanticGuidelineAssessmentUseCase",
    "GetSemanticMetricWaiverCommand",
    "GetSemanticMetricWaiverResult",
    "GetSemanticMetricWaiverUseCase",
    "GetSemanticPolicySkipCommand",
    "GetSemanticPolicySkipResult",
    "GetSemanticPolicySkipUseCase",
    "ListSemanticGuidelineAssessmentsCommand",
    "ListSemanticGuidelineAssessmentsResult",
    "ListSemanticGuidelineAssessmentsUseCase",
    "ListSemanticGuidelineFindingsCommand",
    "ListSemanticGuidelineFindingsResult",
    "ListSemanticGuidelineFindingsUseCase",
    "ListSemanticMetricWaiverEventsCommand",
    "ListSemanticMetricWaiverEventsResult",
    "ListSemanticMetricWaiverEventsUseCase",
    "ListSemanticMetricWaiversCommand",
    "ListSemanticMetricWaiversResult",
    "ListSemanticMetricWaiversUseCase",
    "ListSemanticPolicySkipsCommand",
    "ListSemanticPolicySkipsResult",
    "ListSemanticPolicySkipsUseCase",
    "RequestSemanticMetricWaiverCommand",
    "RequestSemanticMetricWaiverUseCase",
    "ReviewSemanticMetricWaiverCommand",
    "ReviewSemanticMetricWaiverUseCase",
    "RevalidateSemanticMetricWaiverCommand",
    "RevalidateSemanticMetricWaiverResult",
    "RevalidateSemanticMetricWaiverUseCase",
    "RevokeSemanticMetricWaiverCommand",
    "RevokeSemanticMetricWaiverUseCase",
    "RevokeSemanticPolicySkipCommand",
    "RevokeSemanticPolicySkipUseCase",
    "SemanticMetricWaiverMutationResult",
    "SemanticPolicySkipMutationResult",
]
