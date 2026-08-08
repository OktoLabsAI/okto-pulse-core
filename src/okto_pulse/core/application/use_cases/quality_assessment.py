"""Application orchestration for SK-A quality-assessment commands.

Unlike ordinary transaction-scoped CRUD use cases, this coordinator performs
the read-only subject/authority preflight and pure Core preparation *before*
asking the edition to open a writable UoW.
"""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.quality_assessment import (
    AssessmentCommitResult,
    AssessmentCurrentness,
    AssessmentKind,
    AssessmentPreflight,
    AssessmentPreflightRequest,
    AssessmentReceipt,
    AssessmentReceiptState,
    AssessmentSubjectIdentity,
    AssessmentSubjectType,
    AssessmentSubmission,
    FindingAnchor,
    FindingSeverity,
    ProposedQuestionDraft,
    QualityFindingDraft,
    QualityPage,
    QualityPageCursor,
    UnboundQualityFindingDraft,
)
from okto_pulse.core.ports.quality_assessment import (
    AssessmentListQuery,
    AssessmentReadAccessDenied,
    AssessmentReceiptNotFound,
    AssessmentSubjectNotFound,
    FindingListQuery,
    QualityAssessmentPreflightReadPort,
    QualityAssessmentReadContext,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import UnitOfWorkFactory
from okto_pulse.core.services.quality_assessment import (
    QualityAssessmentPortContractError,
    QualityAssessmentService,
    QualityAssessmentForbiddenError,
    QualityAssessmentNotFoundError,
    CurrentAssessmentView,
)


@dataclass(frozen=True, slots=True)
class SubmitQualityAssessmentCommand:
    submission: AssessmentSubmission


@dataclass(frozen=True, slots=True)
class SubmitQualityAssessmentResult:
    receipt_id: str
    head_revision: int
    qa_id_map: tuple[tuple[str, str], ...]
    replayed: bool

    @classmethod
    def from_commit(
        cls,
        result: AssessmentCommitResult,
    ) -> "SubmitQualityAssessmentResult":
        return cls(
            receipt_id=result.receipt_id,
            head_revision=result.head_revision,
            qa_id_map=result.qa_id_map,
            replayed=result.replayed,
        )


class SubmitQualityAssessmentUseCase:
    """Preflight outside the write UoW, then commit the whole bundle once."""

    def __init__(
        self,
        *,
        preflight_reader: QualityAssessmentPreflightReadPort,
        uow_factory: UnitOfWorkFactory,
        service: QualityAssessmentService | None = None,
    ) -> None:
        self._preflight_reader = preflight_reader
        self._uow_factory = uow_factory
        self._service = service or QualityAssessmentService()

    async def execute(
        self,
        command: SubmitQualityAssessmentCommand,
        *,
        actor: ActorContext,
    ) -> SubmitQualityAssessmentResult:
        submission = command.submission
        self._service.validate_submission_envelope(
            submission,
            actor_id=actor.actor_id,
        )
        if actor.board_id != submission.board_id:
            raise QualityAssessmentForbiddenError(
                "assessment_board_scope_mismatch",
                "The authenticated actor is not bound to the assessment board.",
            )
        realm_scope = actor.require_realm_scope()
        preflight = await self._preflight_reader.resolve_assessment_preflight(
            submission,
            actor_id=actor.actor_id,
            realm_scope=realm_scope,
        )
        if not isinstance(preflight, AssessmentPreflight):
            raise QualityAssessmentPortContractError(
                "assessment_preflight_result_invalid"
            )
        self._service.validate_replay_authority(
            submission,
            preflight=preflight,
        )
        replay = await self._preflight_reader.lookup_assessment_replay(
            board_id=submission.board_id,
            idempotency_key=submission.idempotency_key,
            actor_id=actor.actor_id,
            realm_scope=realm_scope,
        )
        if replay is not None:
            return SubmitQualityAssessmentResult.from_commit(
                self._service.resolve_replay(
                    submission,
                    actor_id=actor.actor_id,
                    result=replay,
                )
            )
        bundle = self._service.prepare_submission(
            submission,
            actor_id=actor.actor_id,
            preflight=preflight,
        )
        async with self._uow_factory(
            realm_scope=realm_scope,
            actor=actor,
        ) as uow:
            committed = await self._service.commit_prepared(
                bundle,
                persistence=uow.services.quality_assessments,
            )
            await uow.commit()
        return SubmitQualityAssessmentResult.from_commit(committed)


@dataclass(frozen=True, slots=True)
class RecordAmbiguityAssessmentCommand:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    idempotency_key: str
    expected_subject_version: int
    expected_head_revision: int
    score: float
    findings: tuple[UnboundQualityFindingDraft, ...] = ()
    proposed_questions: tuple[ProposedQuestionDraft, ...] = ()


class RecordAmbiguityAssessmentUseCase:
    """Materialize every server-owned assessment field after preflight."""

    def __init__(
        self,
        *,
        preflight_reader: QualityAssessmentPreflightReadPort,
        uow_factory: UnitOfWorkFactory,
        service: QualityAssessmentService | None = None,
    ) -> None:
        self._preflight_reader = preflight_reader
        self._uow_factory = uow_factory
        self._service = service or QualityAssessmentService()

    async def execute(
        self,
        command: RecordAmbiguityAssessmentCommand,
        *,
        actor: ActorContext,
    ) -> SubmitQualityAssessmentResult:
        if actor.board_id != command.board_id:
            raise QualityAssessmentForbiddenError(
                "assessment_board_scope_mismatch"
            )
        request = AssessmentPreflightRequest(
            board_id=command.board_id,
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            assessment_kind=AssessmentKind.AMBIGUITY,
            expected_subject_version=command.expected_subject_version,
            expected_head_revision=command.expected_head_revision,
            channel=_quality_channel(actor),
        )
        realm_scope = actor.require_realm_scope()
        try:
            preflight = (
                await self._preflight_reader.resolve_assessment_preflight_request(
                    request,
                    actor_id=actor.actor_id,
                    realm_scope=realm_scope,
                )
            )
        except AssessmentSubjectNotFound as exc:
            raise QualityAssessmentNotFoundError(
                "assessment_subject_not_found"
            ) from exc
        except AssessmentReadAccessDenied as exc:
            raise QualityAssessmentForbiddenError(
                "assessment_permission_denied"
            ) from exc
        if not isinstance(preflight, AssessmentPreflight):
            raise QualityAssessmentPortContractError(
                "assessment_preflight_result_invalid"
            )
        submission = AssessmentSubmission(
            board_id=command.board_id,
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            assessment_kind=AssessmentKind.AMBIGUITY,
            idempotency_key=command.idempotency_key,
            expected_subject_version=command.expected_subject_version,
            expected_head_revision=command.expected_head_revision,
            score=command.score,
            justification="Ambiguity assessment recorded.",
            scale=preflight.expected_scale,
            findings=tuple(
                _bind_finding(finding, preflight=preflight)
                for finding in command.findings
            ),
            proposed_questions=tuple(command.proposed_questions),
        )
        self._service.validate_submission_envelope(
            submission,
            actor_id=actor.actor_id,
        )
        self._service.validate_replay_authority(
            submission,
            preflight=preflight,
        )
        replay = await self._preflight_reader.lookup_assessment_replay(
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            actor_id=actor.actor_id,
            realm_scope=realm_scope,
        )
        if replay is not None:
            return SubmitQualityAssessmentResult.from_commit(
                self._service.resolve_replay(
                    submission,
                    actor_id=actor.actor_id,
                    result=replay,
                )
            )
        bundle = self._service.prepare_submission(
            submission,
            actor_id=actor.actor_id,
            preflight=preflight,
        )
        async with self._uow_factory(
            realm_scope=realm_scope,
            actor=actor,
        ) as uow:
            committed = await self._service.commit_prepared(
                bundle,
                persistence=uow.services.quality_assessments,
            )
            await uow.commit()
        return SubmitQualityAssessmentResult.from_commit(committed)


@dataclass(frozen=True, slots=True)
class GetCurrentQualityAssessmentCommand:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    assessment_kind: AssessmentKind


@dataclass(frozen=True, slots=True)
class GetQualityAssessmentReceiptCommand:
    receipt_id: str
    board_id: str | None = None


@dataclass(frozen=True, slots=True)
class GetQualityAssessmentReceiptResult:
    receipt: AssessmentReceipt
    currentness: AssessmentCurrentness


@dataclass(frozen=True, slots=True)
class ListQualityAssessmentsCommand:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    offset: int = 0
    limit: int = 50
    assessment_kind: AssessmentKind | None = None
    state: AssessmentReceiptState | None = None
    cursor: QualityPageCursor | None = None


@dataclass(frozen=True, slots=True)
class ListQualityFindingsCommand:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    offset: int = 0
    limit: int = 50
    receipt_id: str | None = None
    assessment_kind: AssessmentKind | None = None
    category_code: str | None = None
    severity: FindingSeverity | None = None
    cursor: QualityPageCursor | None = None


@dataclass(frozen=True, slots=True)
class ListQualityReceiptFindingsCommand:
    """Receipt-owned findings query with no caller-supplied board scope."""

    receipt_id: str
    offset: int = 0
    limit: int = 50
    category_code: str | None = None
    severity: FindingSeverity | None = None
    cursor: QualityPageCursor | None = None


class QualityAssessmentReadUseCases:
    """Shared REST/MCP read coordinator with pre-UoW authorization."""

    def __init__(
        self,
        *,
        preflight_reader: QualityAssessmentPreflightReadPort,
        uow_factory: UnitOfWorkFactory,
        service: QualityAssessmentService | None = None,
    ) -> None:
        self._preflight_reader = preflight_reader
        self._uow_factory = uow_factory
        self._service = service or QualityAssessmentService()

    async def get_current(
        self,
        command: GetCurrentQualityAssessmentCommand,
        *,
        actor: ActorContext,
    ) -> CurrentAssessmentView:
        context = await self._subject_context(
            board_id=command.board_id,
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            actor=actor,
        )
        async with self._uow_factory(
            realm_scope=actor.require_realm_scope(),
            actor=actor,
        ) as uow:
            return await self._service.get_current(
                board_id=command.board_id,
                subject_type=command.subject_type,
                subject_id=command.subject_id,
                assessment_kind=command.assessment_kind,
                current_subject=context.subject,
                currentness_inputs=context.currentness_inputs,
                gate_inputs=context.gate_inputs,
                persistence=uow.services.quality_assessments,
            )

    async def get_receipt(
        self,
        command: GetQualityAssessmentReceiptCommand,
        *,
        actor: ActorContext,
    ) -> GetQualityAssessmentReceiptResult:
        context = await self._receipt_context(command, actor=actor)
        async with self._uow_factory(
            realm_scope=actor.require_realm_scope(),
            actor=actor,
        ) as uow:
            receipt = await self._service.get_receipt(
                board_id=context.subject.board_id,
                receipt_id=command.receipt_id,
                persistence=uow.services.quality_assessments,
            )
        return GetQualityAssessmentReceiptResult(
            receipt=receipt,
            currentness=self._service.currentness_for_receipt(
                receipt,
                current_subject=context.subject,
                currentness_inputs=context.currentness_inputs,
            ),
        )

    async def list_assessments(
        self,
        command: ListQualityAssessmentsCommand,
        *,
        actor: ActorContext,
    ) -> QualityPage:
        context = await self._subject_context(
            board_id=command.board_id,
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            actor=actor,
        )
        offset = _quality_page_offset(command.offset, command.cursor)
        query = AssessmentListQuery(
            subject=AssessmentSubjectIdentity(
                board_id=command.board_id,
                subject_type=command.subject_type,
                subject_id=command.subject_id,
            ),
            offset=offset,
            limit=command.limit,
            assessment_kind=command.assessment_kind,
            state=command.state,
            current_subject_version=context.subject.subject_version,
            currentness_inputs=context.currentness_inputs,
            cursor=command.cursor,
        )
        async with self._uow_factory(
            realm_scope=actor.require_realm_scope(),
            actor=actor,
        ) as uow:
            return await self._service.list_assessments(
                query,
                persistence=uow.services.quality_assessments,
            )

    async def list_findings(
        self,
        command: ListQualityFindingsCommand,
        *,
        actor: ActorContext,
    ) -> QualityPage:
        await self._subject_context(
            board_id=command.board_id,
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            actor=actor,
        )
        return await self._list_findings_authorized(command, actor=actor)

    async def _list_findings_authorized(
        self,
        command: ListQualityFindingsCommand,
        *,
        actor: ActorContext,
    ) -> QualityPage:
        """Query findings after one of the literal read preflights succeeded."""

        offset = _quality_page_offset(command.offset, command.cursor)
        query = FindingListQuery(
            board_id=command.board_id,
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            offset=offset,
            limit=command.limit,
            receipt_id=command.receipt_id,
            assessment_kind=command.assessment_kind,
            category_code=command.category_code,
            severity=command.severity,
            cursor=command.cursor,
        )
        async with self._uow_factory(
            realm_scope=actor.require_realm_scope(),
            actor=actor,
        ) as uow:
            return await self._service.list_findings(
                query,
                persistence=uow.services.quality_assessments,
            )

    async def list_receipt_findings(
        self,
        command: ListQualityReceiptFindingsCommand,
        *,
        actor: ActorContext,
    ) -> QualityPage:
        if not isinstance(command.receipt_id, str) or not command.receipt_id.strip():
            raise ValueError("assessment_receipt_id_required")
        context = await self._receipt_context(
            GetQualityAssessmentReceiptCommand(
                receipt_id=command.receipt_id,
            ),
            actor=actor,
        )
        scoped = ListQualityFindingsCommand(
            board_id=context.subject.board_id,
            subject_type=context.subject.subject_type,
            subject_id=context.subject.subject_id,
            offset=command.offset,
            limit=command.limit,
            receipt_id=command.receipt_id,
            category_code=command.category_code,
            severity=command.severity,
            cursor=command.cursor,
        )
        return await self._list_findings_authorized(scoped, actor=actor)

    async def _subject_context(
        self,
        *,
        board_id: str,
        subject_type: AssessmentSubjectType,
        subject_id: str,
        actor: ActorContext,
    ) -> QualityAssessmentReadContext:
        try:
            context = (
                await self._preflight_reader.resolve_assessment_read_context(
                    board_id=board_id,
                    subject_type=subject_type,
                    subject_id=subject_id,
                    actor_id=actor.actor_id,
                    realm_scope=actor.require_realm_scope(),
                )
            )
        except AssessmentSubjectNotFound as exc:
            raise QualityAssessmentNotFoundError(
                "assessment_subject_not_found"
            ) from exc
        except AssessmentReadAccessDenied as exc:
            raise QualityAssessmentForbiddenError(
                "assessment_read_permission_denied"
            ) from exc
        if not isinstance(context, QualityAssessmentReadContext):
            raise QualityAssessmentPortContractError(
                "assessment_read_context_invalid"
            )
        return context

    async def _receipt_context(
        self,
        command: GetQualityAssessmentReceiptCommand,
        *,
        actor: ActorContext,
    ) -> QualityAssessmentReadContext:
        try:
            context = (
                await self._preflight_reader.resolve_receipt_read_context(
                    receipt_id=command.receipt_id,
                    board_id=command.board_id,
                    actor_id=actor.actor_id,
                    realm_scope=actor.require_realm_scope(),
                )
            )
        except AssessmentReceiptNotFound as exc:
            raise QualityAssessmentNotFoundError(
                "assessment_receipt_not_found"
            ) from exc
        except AssessmentReadAccessDenied as exc:
            raise QualityAssessmentForbiddenError(
                "assessment_read_permission_denied"
            ) from exc
        if not isinstance(context, QualityAssessmentReadContext):
            raise QualityAssessmentPortContractError(
                "assessment_read_context_invalid"
            )
        return context


def _quality_channel(actor: ActorContext) -> str:
    if actor.source == "rest":
        return "rest:quality_assess"
    if actor.source == "mcp":
        return "mcp:quality_assess"
    raise QualityAssessmentForbiddenError(
        "assessment_channel_unsupported"
    )


def _bind_finding(
    draft: UnboundQualityFindingDraft,
    *,
    preflight: AssessmentPreflight,
) -> QualityFindingDraft:
    subject = preflight.subject
    return QualityFindingDraft(
        finding_key=draft.finding_key,
        category_code=draft.category_code,
        severity=draft.severity,
        confidence=draft.confidence,
        deterministic=draft.deterministic,
        blocking_eligible=False,
        title=draft.title,
        detail=draft.detail,
        anchor=FindingAnchor(
            board_id=subject.board_id,
            subject_type=subject.subject_type,
            subject_id=subject.subject_id,
            subject_version=subject.subject_version,
            input_digest=str(preflight.digests.input_digest),
            anchor_type=draft.anchor.anchor_type,
            anchor_ref=draft.anchor.anchor_ref,
            excerpt_hash=draft.anchor.excerpt_hash,
        ),
        remediation=draft.remediation,
        rule_code=draft.rule_code,
        evidence_refs=draft.evidence_refs,
    )


def _quality_page_offset(
    offset: int,
    cursor: QualityPageCursor | None,
) -> int:
    if cursor is None:
        return offset
    if offset != 0:
        raise ValueError("quality_cursor_offset_conflict")
    return cursor.offset


__all__ = [
    "GetCurrentQualityAssessmentCommand",
    "GetQualityAssessmentReceiptCommand",
    "GetQualityAssessmentReceiptResult",
    "ListQualityAssessmentsCommand",
    "ListQualityFindingsCommand",
    "ListQualityReceiptFindingsCommand",
    "QualityAssessmentReadUseCases",
    "RecordAmbiguityAssessmentCommand",
    "RecordAmbiguityAssessmentUseCase",
    "SubmitQualityAssessmentCommand",
    "SubmitQualityAssessmentResult",
    "SubmitQualityAssessmentUseCase",
]
