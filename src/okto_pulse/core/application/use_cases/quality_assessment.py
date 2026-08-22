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
from okto_pulse.core.domain.validation_cycle import RequirementLintPreflight
from okto_pulse.core.domain.requirement_lint import (
    external_requirement_lint_rule_capacity_v1,
    external_requirement_lint_scale_v1,
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
    QualityAssessmentConflictError,
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
    subject_edition: int | None = None

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
            subject_edition=result.subject_edition,
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
    expected_subject_edition: int
    expected_head_revision: int
    score: float
    summary: str = "Ambiguity assessment recorded."
    findings: tuple[UnboundQualityFindingDraft, ...] = ()
    proposed_questions: tuple[ProposedQuestionDraft, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.summary, str) or not self.summary.strip():
            raise ValueError("ambiguity_assessment_summary_required")
        object.__setattr__(self, "summary", self.summary.strip())


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
            expected_subject_edition=command.expected_subject_edition,
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
            expected_subject_edition=command.expected_subject_edition,
            expected_head_revision=command.expected_head_revision,
            score=command.score,
            justification=command.summary,
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
class RecordRequirementLintCommand:
    board_id: str
    spec_id: str
    idempotency_key: str
    expected_subject_version: int
    expected_subject_edition: int
    expected_head_revision: int
    ruleset_digest: str
    score: float
    summary: str
    findings: tuple[UnboundQualityFindingDraft, ...] = ()
    # Legacy compatibility only. Canonical clients derive this fence from the
    # pinned ruleset plus requirement anchors returned by preflight.
    evaluated_rule_count: int | None = None

    def __post_init__(self) -> None:
        for field_name in ("board_id", "spec_id", "idempotency_key", "summary"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"requirement_lint_{field_name}_required")
            object.__setattr__(self, field_name, value.strip())
        for field_name in ("expected_subject_version", "expected_subject_edition"):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"requirement_lint_{field_name}_invalid")
        if (
            not isinstance(self.expected_head_revision, int)
            or isinstance(self.expected_head_revision, bool)
            or self.expected_head_revision < 0
        ):
            raise ValueError("requirement_lint_expected_head_revision_invalid")
        if self.evaluated_rule_count is not None and (
            not isinstance(self.evaluated_rule_count, int)
            or isinstance(self.evaluated_rule_count, bool)
            or self.evaluated_rule_count < 1
        ):
            raise ValueError("requirement_lint_evaluated_rule_count_invalid")
        if (
            not isinstance(self.score, (int, float))
            or isinstance(self.score, bool)
            or float(self.score) < 0
        ):
            raise ValueError("requirement_lint_score_invalid")
        normalized_digest = self.ruleset_digest.strip().lower() if isinstance(
            self.ruleset_digest, str
        ) else ""
        if len(normalized_digest) != 64:
            raise ValueError("requirement_lint_ruleset_digest_invalid")
        try:
            int(normalized_digest, 16)
        except ValueError as exc:
            raise ValueError("requirement_lint_ruleset_digest_invalid") from exc
        object.__setattr__(self, "ruleset_digest", normalized_digest)
        findings = tuple(self.findings)
        if any(not isinstance(item, UnboundQualityFindingDraft) for item in findings):
            raise ValueError("requirement_lint_findings_invalid")
        object.__setattr__(self, "findings", findings)
        if float(self.score) != float(len(findings)):
            raise ValueError("requirement_lint_score_findings_mismatch")


class RecordRequirementLintUseCase:
    """Record externally-produced lint evidence for one approved Spec edition."""

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
        command: RecordRequirementLintCommand,
        *,
        actor: ActorContext,
    ) -> SubmitQualityAssessmentResult:
        if actor.board_id != command.board_id:
            raise QualityAssessmentForbiddenError(
                "assessment_board_scope_mismatch"
            )
        realm_scope = actor.require_realm_scope()
        lint_preflight = (
            await self._preflight_reader.resolve_requirement_lint_preflight(
                spec_id=command.spec_id,
                actor_id=actor.actor_id,
                realm_scope=realm_scope,
            )
        )
        fence = lint_preflight.submission_fence
        if (
            lint_preflight.subject_edition != command.expected_subject_edition
            or fence.expected_subject_version
            != command.expected_subject_version
        ):
            raise QualityAssessmentConflictError(
                "requirement_lint_submission_fence_conflict"
            )
        if lint_preflight.ruleset_digest != command.ruleset_digest:
            raise QualityAssessmentConflictError(
                "requirement_lint_ruleset_conflict"
            )
        derived_rule_count = external_requirement_lint_rule_capacity_v1(
            len(lint_preflight.requirement_anchors)
        )
        evaluated_rule_count = (
            derived_rule_count
            if command.evaluated_rule_count is None
            else command.evaluated_rule_count
        )
        request = AssessmentPreflightRequest(
            board_id=command.board_id,
            subject_type=AssessmentSubjectType.SPEC,
            subject_id=command.spec_id,
            assessment_kind=AssessmentKind.REQUIREMENT_LINT,
            expected_subject_version=command.expected_subject_version,
            expected_subject_edition=command.expected_subject_edition,
            # Resolve current authority/digests with the live mutable head. The
            # immutable submission below retains the caller's original head so
            # an exact idempotent retry can be authenticated by fingerprint
            # before the changed head is treated as a new-write conflict.
            expected_head_revision=fence.expected_head_revision,
            channel=_quality_channel(actor),
            evaluated_rule_count=evaluated_rule_count,
        )
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
        if preflight.expected_scale != external_requirement_lint_scale_v1(
            evaluated_rule_count
        ):
            raise QualityAssessmentPortContractError(
                "requirement_lint_preflight_scale_invalid"
            )
        if preflight.digests.ruleset_digest != command.ruleset_digest:
            raise QualityAssessmentConflictError(
                "requirement_lint_ruleset_conflict"
            )
        submission = AssessmentSubmission(
            board_id=command.board_id,
            subject_type=AssessmentSubjectType.SPEC,
            subject_id=command.spec_id,
            assessment_kind=AssessmentKind.REQUIREMENT_LINT,
            idempotency_key=command.idempotency_key,
            expected_subject_version=command.expected_subject_version,
            expected_subject_edition=command.expected_subject_edition,
            expected_head_revision=command.expected_head_revision,
            score=float(command.score),
            justification=command.summary,
            scale=preflight.expected_scale,
            findings=tuple(
                _bind_finding(finding, preflight=preflight)
                for finding in command.findings
            ),
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
        if fence.expected_head_revision != command.expected_head_revision:
            raise QualityAssessmentConflictError(
                "requirement_lint_submission_fence_conflict"
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
class GetRequirementLintPreflightCommand:
    spec_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.spec_id, str) or not self.spec_id.strip():
            raise ValueError("requirement_lint_spec_id_required")
        object.__setattr__(self, "spec_id", self.spec_id.strip())


class GetRequirementLintPreflightUseCase:
    """Expose the minimal read-only fence required by REST/UI/MCP clients."""

    def __init__(
        self,
        *,
        preflight_reader: QualityAssessmentPreflightReadPort,
    ) -> None:
        self._preflight_reader = preflight_reader

    async def execute(
        self,
        command: GetRequirementLintPreflightCommand,
        *,
        actor: ActorContext,
    ) -> RequirementLintPreflight:
        result = await self._preflight_reader.resolve_requirement_lint_preflight(
            spec_id=command.spec_id,
            actor_id=actor.actor_id,
            realm_scope=actor.require_realm_scope(),
        )
        if not isinstance(result, RequirementLintPreflight):
            raise QualityAssessmentPortContractError(
                "requirement_lint_preflight_result_invalid"
            )
        return result


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
            current_subject_edition=context.subject.subject_edition,
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
    "GetRequirementLintPreflightCommand",
    "GetRequirementLintPreflightUseCase",
    "GetCurrentQualityAssessmentCommand",
    "GetQualityAssessmentReceiptCommand",
    "GetQualityAssessmentReceiptResult",
    "ListQualityAssessmentsCommand",
    "ListQualityFindingsCommand",
    "ListQualityReceiptFindingsCommand",
    "QualityAssessmentReadUseCases",
    "RecordAmbiguityAssessmentCommand",
    "RecordAmbiguityAssessmentUseCase",
    "RecordRequirementLintCommand",
    "RecordRequirementLintUseCase",
    "SubmitQualityAssessmentCommand",
    "SubmitQualityAssessmentResult",
    "SubmitQualityAssessmentUseCase",
]
