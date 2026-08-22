from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.service_catalog import (
    build_application_service_catalog,
)
from okto_pulse.core.application.use_cases.quality_assessment import (
    RecordRequirementLintCommand,
    RecordRequirementLintUseCase,
    SubmitQualityAssessmentCommand,
    SubmitQualityAssessmentUseCase,
)
from okto_pulse.core.domain.quality_assessment import (
    AssessmentAnchorCatalog,
    AssessmentAuthoritySnapshot,
    AssessmentCommitResult,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentOutcome,
    AssessmentPreflight,
    AssessmentReceiptDetail,
    AssessmentScale,
    AssessmentScaleKind,
    AssessmentSource,
    AssessmentSubjectHead,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentSubmission,
    AssessmentVersionSet,
    FindingAnchor,
    FindingAnchorType,
    FindingSeverity,
    ProposedQuestionDraft,
    QualityAssessmentContractError,
    QualityFindingDraft,
    QualityPage,
    ScoreDirection,
    project_assessment_receipt_view,
)
from okto_pulse.core.domain.realm import MissingRealmScope, RealmScope
from okto_pulse.core.domain.requirement_lint import (
    external_requirement_lint_scale_v1,
)
from okto_pulse.core.domain.validation_cycle import (
    RequirementLintPreflight,
    ValidationSubmissionFence,
)
from okto_pulse.core.events.types import QualityAssessmentRecorded
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.ports.quality_assessment import (
    AssessmentHeadRevisionConflict,
    AssessmentListQuery,
    AssessmentSubjectLifecycleConflict,
    FindingListQuery,
    QualityGateInput,
)
from okto_pulse.core.ports.relational_application import (
    register_relational_application_adapter,
    reset_relational_application_adapter_for_tests,
)
from okto_pulse.core.services.quality_assessment import (
    QualityAssessmentConflictError,
    QualityAssessmentForbiddenError,
    QualityAssessmentPortContractError,
    QualityAssessmentService,
    QualityAssessmentValidationError,
)

NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
AUTHORITY_DIGEST = canonical_sha256("authority")


def _digests() -> AssessmentDigestSet:
    return AssessmentDigestSet(
        content_digest=canonical_sha256("content"),
        clarification_digest=canonical_sha256("clarification"),
        ruleset_digest=canonical_sha256("ruleset"),
        taxonomy_digest=canonical_sha256("taxonomy"),
        policy_digest=canonical_sha256("policy"),
    )


def _preflight(
    *,
    authority: AssessmentAuthoritySnapshot | None = None,
    head_revision: int = 3,
    channel: str = "mcp",
) -> AssessmentPreflight:
    return AssessmentPreflight(
        subject=AssessmentSubjectRef(
            board_id="b1",
            subject_type=AssessmentSubjectType.REFINEMENT,
            subject_id="r1",
            subject_version=7,
        ),
        status="approved",
        current_head_revision=head_revision,
        current_head_receipt_id=(
            None if head_revision == 0 else f"qar_head_{head_revision}"
        ),
        channel=channel,
        expected_scale=AssessmentScale(
            AssessmentScaleKind.AMBIGUITY_SCORE,
            1,
            5,
            ScoreDirection.LOWER_BETTER,
        ),
        digests=_digests(),
        versions=AssessmentVersionSet(
            "rules/v1",
            "taxonomy/v1",
            "analyzer/v1",
            "policy/v1",
        ),
        anchors=AssessmentAnchorCatalog(
            fields=frozenset({"analysis"}),
            structured_child_ids=frozenset({"decision_1"}),
            qa_ids=frozenset({"qa_existing"}),
        ),
        allowed_category_codes=frozenset({"acceptance_measurability"}),
        authority=authority
        or AssessmentAuthoritySnapshot(
            domain_write=True,
            quality_assess=True,
            qa_ask=True,
            authority_digest=AUTHORITY_DIGEST,
        ),
        origin=AssessmentOrigin.HUMAN_OR_AGENT,
    )


def _finding(preflight: AssessmentPreflight | None = None) -> QualityFindingDraft:
    resolved = preflight or _preflight()
    return QualityFindingDraft(
        finding_key="f1",
        category_code="acceptance_measurability",
        severity=FindingSeverity.MEDIUM,
        confidence=0.95,
        deterministic=True,
        blocking_eligible=False,
        title="Ambiguous result",
        detail="Define an observable result.",
        anchor=FindingAnchor(
            board_id="b1",
            subject_type=AssessmentSubjectType.REFINEMENT,
            subject_id="r1",
            subject_version=7,
            input_digest=resolved.digests.input_digest or "",
            anchor_type=FindingAnchorType.FIELD,
            anchor_ref="analysis",
        ),
    )


def _submission(
    *,
    findings=None,
    questions=(),
    expected_head_revision: int = 3,
) -> AssessmentSubmission:
    return AssessmentSubmission(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        assessment_kind=AssessmentKind.AMBIGUITY,
        idempotency_key="idem-1",
        expected_subject_version=7,
        expected_head_revision=expected_head_revision,
        score=2,
        justification="The score is supported by the attached finding.",
        scale=AssessmentScale(
            AssessmentScaleKind.AMBIGUITY_SCORE,
            1,
            5,
            ScoreDirection.LOWER_BETTER,
        ),
        findings=tuple(findings if findings is not None else [_finding()]),
        proposed_questions=tuple(questions),
    )


class _Ids:
    def __init__(self) -> None:
        self.counter = 0

    def __call__(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}_{self.counter}"


class _Persistence:
    def __init__(self) -> None:
        self.bundles = []
        self.result = None
        self.error = None

    async def apply_bundle_cas(self, bundle):
        self.bundles.append(bundle)
        if self.error:
            raise self.error
        return self.result or AssessmentCommitResult(
            board_id=bundle.receipt.subject.board_id,
            subject_type=bundle.receipt.subject.subject_type,
            subject_id=bundle.receipt.subject.subject_id,
            subject_version=bundle.receipt.subject.subject_version,
            assessment_kind=bundle.receipt.assessment_kind,
            request_fingerprint=bundle.request_fingerprint,
            receipt_id=bundle.receipt.id,
            head_revision=bundle.next_head.revision,
            event_id=bundle.audit_intent.event_id,
            history_id=bundle.audit_intent.history_id,
            outbox_id=bundle.audit_intent.outbox_id,
            qa_id_map=tuple(
                (item.client_key, item.qa_id)
                for item in bundle.proposed_questions
            ),
        )

    async def get_current(self, **kwargs):
        return None

    async def get_receipt(self, **kwargs):
        return None

    async def get_receipt_detail(self, **kwargs):
        return None

    async def list_assessments(self, query):
        return QualityPage((), 0, 0, query.offset, query.limit)

    async def list_findings(self, query):
        return QualityPage((), 0, 0, query.offset, query.limit)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "enabled",
        "skipped",
        "threshold",
        "subject_version",
        "expected_allowed",
        "expected_reason",
    ),
    (
        (False, False, 3, 7, True, "ambiguity_gate_disabled"),
        (True, True, 3, 7, True, "ambiguity_gate_skipped"),
        (True, False, 1, 7, False, "ambiguity_score_exceeds_threshold"),
        (True, False, 3, 7, True, "ambiguity_gate_ready"),
        (True, False, 3, 8, False, "ambiguity_assessment_stale"),
    ),
)
async def test_current_assessment_projects_the_canonical_gate_preview(
    enabled: bool,
    skipped: bool,
    threshold: int,
    subject_version: int,
    expected_allowed: bool,
    expected_reason: str,
) -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    head = AssessmentSubjectHead(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        assessment_kind=AssessmentKind.AMBIGUITY,
        receipt_id=bundle.receipt.id,
        revision=4,
        updated_at=NOW,
    )

    class _Current(_Persistence):
        async def get_current(self, **kwargs):
            return bundle.receipt, head

    view = await service.get_current(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        assessment_kind=AssessmentKind.AMBIGUITY,
        current_subject=replace(
            _preflight().subject,
            subject_version=subject_version,
        ),
        current_digests=_digests(),
        gate_inputs=(
            QualityGateInput(
                assessment_kind=AssessmentKind.AMBIGUITY,
                applicable=True,
                enabled=enabled,
                threshold=threshold,
                skipped=skipped,
            ),
        ),
        persistence=_Current(),
    )

    assert view.gate_preview.allowed is expected_allowed
    assert view.gate_preview.reason_code == expected_reason
    assert view.gate_preview.threshold == threshold
    assert view.gate_preview.score == 2


def test_prepare_builds_one_coherent_immutable_bundle() -> None:
    ids = _Ids()
    service = QualityAssessmentService(id_factory=ids, clock=lambda: NOW)
    preflight = _preflight(channel="rest")
    question = ProposedQuestionDraft(
        client_key="q1",
        question="What is observable?",
        question_type="free_text",
        finding_keys=("f1",),
    )

    bundle = service.prepare_submission(
        _submission(questions=(question,)),
        actor_id="agent-1",
        preflight=preflight,
    )

    assert bundle.receipt.id == "qar_1"
    assert [item.id for item in bundle.findings] == ["qf_2"]
    assert [item.qa_id for item in bundle.proposed_questions] == ["qa_3"]
    assert bundle.finding_qa_links[0].finding_id == "qf_2"
    assert bundle.finding_qa_links[0].qa_id == "qa_3"
    assert bundle.next_head.revision == 4
    assert bundle.next_head.receipt_id == bundle.receipt.id
    assert bundle.expected_head_receipt_id == "qar_head_3"
    assert bundle.receipt.predecessor_receipt_id == "qar_head_3"
    assert bundle.receipt.source is AssessmentSource.NATIVE
    assert bundle.receipt.channel == "rest"
    assert bundle.receipt.outcome is AssessmentOutcome.RECORDED
    assert bundle.receipt.justification == (
        "The score is supported by the attached finding."
    )
    assert bundle.receipt.idempotency_key == "idem-1"
    assert bundle.receipt.request_digest == bundle.request_fingerprint
    assert bundle.receipt.versions.analyzer_version == "analyzer/v1"
    assert bundle.receipt.authority_digest == AUTHORITY_DIGEST
    assert bundle.findings[0].confidence == 0.95
    assert bundle.findings[0].deterministic is True
    assert bundle.findings[0].blocking_eligible is False
    assert bundle.findings[0].taxonomy_version == "taxonomy/v1"
    assert bundle.audit_intent.event_id == "qae_4"
    assert bundle.audit_intent.history_id == "qah_5"
    assert bundle.audit_intent.outbox_id == "qao_6"


def test_first_assessment_has_no_predecessor_and_starts_revision_one() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    preflight = _preflight(head_revision=0)
    submission = _submission(expected_head_revision=0)

    bundle = service.prepare_submission(
        submission,
        actor_id="agent-1",
        preflight=preflight,
    )

    assert bundle.expected_head_receipt_id is None
    assert bundle.receipt.predecessor_receipt_id is None
    assert bundle.next_head.revision == 1


@pytest.mark.parametrize("confidence", [-0.01, 1.01, float("nan"), True])
def test_finding_confidence_is_closed_to_finite_zero_through_one(
    confidence: object,
) -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="finding_confidence_invalid",
    ):
        replace(_finding(), confidence=confidence)


def test_finding_can_never_be_blocking_eligible_in_a1a_or_a2() -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="finding_blocking_eligibility_forbidden",
    ):
        replace(_finding(), blocking_eligible=True)


def test_requirement_lint_inherits_only_semantic_writer_domain_authority() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    scale = AssessmentScale(
        AssessmentScaleKind.FINDING_COUNT,
        0,
        100,
        ScoreDirection.LOWER_BETTER,
    )
    submission = replace(
        _submission(findings=()),
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="s1",
        assessment_kind=AssessmentKind.REQUIREMENT_LINT,
        score=0,
        scale=scale,
    )
    preflight = replace(
        _preflight(
            authority=AssessmentAuthoritySnapshot(
                domain_write=True,
                quality_assess=False,
                qa_ask=False,
                authority_digest=AUTHORITY_DIGEST,
            )
        ),
        subject=AssessmentSubjectRef(
            board_id="b1",
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="s1",
            subject_version=7,
        ),
        status="in_progress",
        expected_scale=scale,
        origin=AssessmentOrigin.SEMANTIC_WRITER,
    )

    bundle = service.prepare_submission(
        submission,
        actor_id="semantic-writer",
        preflight=preflight,
    )

    assert bundle.receipt.assessment_kind is AssessmentKind.REQUIREMENT_LINT
    assert bundle.receipt.origin is AssessmentOrigin.SEMANTIC_WRITER
    assert bundle.receipt.outcome is AssessmentOutcome.ADVISORY
    assert bundle.findings == ()


def test_wrong_subject_status_is_a_non_retryable_conflict() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)

    with pytest.raises(QualityAssessmentConflictError) as exc_info:
        service.prepare_submission(
            _submission(),
            actor_id="agent-1",
            preflight=replace(_preflight(), status="refining"),
        )

    assert exc_info.value.code == "assessment_subject_status_conflict"
    assert exc_info.value.retryable is False


@pytest.mark.parametrize(
    ("submission", "preflight", "error_type", "code"),
    [
        (
            _submission(findings=()),
            _preflight(),
            QualityAssessmentValidationError,
            "assessment_finding_required",
        ),
        (
            _submission(expected_head_revision=2),
            _preflight(),
            QualityAssessmentConflictError,
            "assessment_head_revision_conflict",
        ),
        (
            _submission(),
            _preflight(
                authority=AssessmentAuthoritySnapshot(
                    domain_write=True,
                    quality_assess=False,
                    authority_digest=AUTHORITY_DIGEST,
                )
            ),
            QualityAssessmentForbiddenError,
            "assessment_permission_denied",
        ),
    ],
)
def test_prepare_fails_before_persistence(submission, preflight, error_type, code) -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    with pytest.raises(error_type) as exc_info:
        service.prepare_submission(
            submission,
            actor_id="agent-1",
            preflight=preflight,
        )
    assert exc_info.value.code == code


def test_proposed_questions_are_evidence_and_do_not_require_qa_write_permission(
) -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    submission = _submission(
        questions=(
            ProposedQuestionDraft(
                client_key="q1",
                question="Why?",
                question_type="free_text",
                finding_keys=("f1",),
            ),
        )
    )
    preflight = _preflight(
        authority=AssessmentAuthoritySnapshot(
            domain_write=True,
            quality_assess=True,
            qa_ask=False,
            authority_digest=AUTHORITY_DIGEST,
        )
    )

    bundle = service.prepare_submission(
        submission,
        actor_id="agent-1",
        preflight=preflight,
    )

    assert len(bundle.proposed_questions) == 1


def test_scale_mismatch_is_rejected_before_persistence() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    submission = replace(
        _submission(),
        scale=AssessmentScale(
            AssessmentScaleKind.PERCENTAGE,
            0,
            100,
            ScoreDirection.HIGHER_BETTER,
        ),
        score=50,
    )
    with pytest.raises(QualityAssessmentValidationError) as exc_info:
        service.prepare_submission(
            submission,
            actor_id="agent-1",
            preflight=_preflight(),
        )
    assert exc_info.value.code == "assessment_scale_mismatch"


def test_unknown_finding_and_question_categories_are_rejected() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    unknown_finding = replace(_finding(), category_code="typo")
    with pytest.raises(QualityAssessmentValidationError) as finding_error:
        service.prepare_submission(
            _submission(findings=(unknown_finding,)),
            actor_id="agent-1",
            preflight=_preflight(),
        )
    assert finding_error.value.code == "finding_category_code_unknown"

    question = ProposedQuestionDraft(
        client_key="q1",
        question="Why?",
        question_type="free_text",
        category_code="typo",
        finding_keys=("f1",),
    )
    with pytest.raises(QualityAssessmentValidationError) as question_error:
        service.prepare_submission(
            _submission(questions=(question,)),
            actor_id="agent-1",
            preflight=_preflight(),
        )
    assert question_error.value.code == "question_category_code_unknown"


def test_archived_subject_is_rejected_before_persistence() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    with pytest.raises(QualityAssessmentConflictError) as exc_info:
        service.prepare_submission(
            _submission(),
            actor_id="agent-1",
            preflight=replace(_preflight(), subject_archived=True),
        )
    assert exc_info.value.code == "assessment_subject_lifecycle_conflict"


def test_question_budget_is_closed_at_five() -> None:
    questions = tuple(
        ProposedQuestionDraft(
            client_key=f"q{i}",
            question=f"Question {i}?",
            question_type="free_text",
        )
        for i in range(6)
    )
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    with pytest.raises(QualityAssessmentValidationError) as exc_info:
        service.prepare_submission(
            _submission(questions=questions),
            actor_id="agent-1",
            preflight=_preflight(),
        )
    assert exc_info.value.code == "question_budget_exceeded"


@pytest.mark.parametrize(
    ("anchor_type", "anchor_ref"),
    [
        (FindingAnchorType.FIELD, "missing"),
        (FindingAnchorType.STRUCTURED_CHILD, "decision_404"),
        (FindingAnchorType.QA, "qa_404"),
    ],
)
def test_anchor_must_resolve_in_the_version_bound_catalog(
    anchor_type,
    anchor_ref,
) -> None:
    preflight = _preflight()
    finding = replace(
        _finding(preflight),
        anchor=replace(
            _finding(preflight).anchor,
            anchor_type=anchor_type,
            anchor_ref=anchor_ref,
        ),
    )
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    with pytest.raises(QualityAssessmentValidationError) as exc_info:
        service.prepare_submission(
            _submission(findings=(finding,)),
            actor_id="agent-1",
            preflight=preflight,
        )
    assert exc_info.value.code == "finding_anchor_not_found"


@pytest.mark.parametrize(
    "anchor_ref",
    [
        "0",
        "[0]",
        "requirements[0]",
        "requirements.0",
        "/items/0/name",
        "items/0/name",
    ],
)
def test_anchor_rejects_mutable_positional_indices_anywhere(anchor_ref) -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="finding_mutable_index_anchor_forbidden",
    ):
        FindingAnchor(
            board_id="b1",
            subject_type=AssessmentSubjectType.REFINEMENT,
            subject_id="r1",
            subject_version=7,
            input_digest=_digests().input_digest or "",
            anchor_type=FindingAnchorType.STRUCTURED_CHILD,
            anchor_ref=anchor_ref,
        )


def test_anchor_allows_stable_ids_containing_digits() -> None:
    anchor = FindingAnchor(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        subject_version=7,
        input_digest=_digests().input_digest or "",
        anchor_type=FindingAnchorType.STRUCTURED_CHILD,
        anchor_ref="fr_1",
    )
    assert anchor.anchor_ref == "fr_1"


def test_bundle_contract_rejects_a_tampered_head() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    with pytest.raises(
        QualityAssessmentContractError,
        match="assessment_bundle_head_mismatch",
    ):
        replace(
            bundle,
            next_head=replace(bundle.next_head, receipt_id="other"),
        )


def test_bundle_contract_rejects_a_finding_from_another_assessment_kind() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )

    with pytest.raises(
        QualityAssessmentContractError,
        match="assessment_bundle_finding_mismatch",
    ):
        replace(
            bundle,
            findings=(
                replace(
                    bundle.findings[0],
                    assessment_kind=AssessmentKind.REQUIREMENT_LINT,
                ),
            ),
        )


@pytest.mark.asyncio
async def test_persistence_cas_conflict_is_mapped_to_canonical_409() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    persistence = _Persistence()
    persistence.error = AssessmentHeadRevisionConflict()

    with pytest.raises(QualityAssessmentConflictError) as exc_info:
        await service.commit_prepared(bundle, persistence=persistence)
    assert exc_info.value.code == "assessment_head_revision_conflict"
    assert exc_info.value.retryable is True


@pytest.mark.asyncio
async def test_archive_race_is_mapped_to_non_retryable_lifecycle_conflict() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    persistence = _Persistence()
    persistence.error = AssessmentSubjectLifecycleConflict()

    with pytest.raises(QualityAssessmentConflictError) as exc_info:
        await service.commit_prepared(bundle, persistence=persistence)
    assert exc_info.value.code == "assessment_subject_lifecycle_conflict"
    assert exc_info.value.retryable is False


class _PreflightReader:
    def __init__(self, value, replay=None) -> None:
        self.value = value
        self.replay = replay
        self.calls = 0

    async def resolve_assessment_preflight(
        self,
        submission,
        *,
        actor_id,
        realm_scope,
    ):
        assert realm_scope == RealmScope.local()
        self.calls += 1
        return self.value

    async def lookup_assessment_replay(
        self,
        *,
        board_id,
        idempotency_key,
        actor_id,
        realm_scope,
    ):
        assert realm_scope == RealmScope.local()
        return self.replay


class _AssessmentUow:
    def __init__(self, persistence) -> None:
        self.services = type(
            "_Services",
            (),
            {"quality_assessments": persistence},
        )()
        self.commits = 0
        self.rollbacks = 0

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


class _UowFactory:
    def __init__(self, persistence) -> None:
        self.uow = _AssessmentUow(persistence)
        self.opens = 0

    @asynccontextmanager
    async def __call__(self, *, realm_scope, actor=None):
        self.opens += 1
        try:
            yield self.uow
        except Exception:
            await self.uow.rollback()
            raise


class _RequirementLintReplayReader:
    def __init__(self) -> None:
        self.head_revision = 0
        self.replay: AssessmentCommitResult | None = None
        self.lookup_calls = 0

    def assessment_preflight(self) -> AssessmentPreflight:
        return AssessmentPreflight(
            subject=AssessmentSubjectRef(
                board_id="b1",
                subject_type=AssessmentSubjectType.SPEC,
                subject_id="spec-1",
                subject_version=7,
                subject_edition=1,
            ),
            status="approved",
            current_head_revision=self.head_revision,
            current_head_receipt_id=(
                None if self.head_revision == 0 else "qar_lint_original"
            ),
            channel="mcp:quality_assess",
            expected_scale=external_requirement_lint_scale_v1(1),
            digests=_digests(),
            versions=AssessmentVersionSet(
                "rules/v1",
                "taxonomy/v1",
                "external-agent/v1",
                "policy/v1",
            ),
            anchors=AssessmentAnchorCatalog(),
            allowed_category_codes=frozenset({"acceptance_measurability"}),
            authority=AssessmentAuthoritySnapshot(
                domain_write=True,
                quality_assess=True,
                authority_digest=AUTHORITY_DIGEST,
            ),
            origin=AssessmentOrigin.HUMAN_OR_AGENT,
        )

    async def resolve_requirement_lint_preflight(
        self,
        *,
        spec_id,
        actor_id,
        realm_scope,
    ):
        assert spec_id == "spec-1"
        assert actor_id == "agent-1"
        assert realm_scope == RealmScope.local()
        preflight = self.assessment_preflight()
        return RequirementLintPreflight(
            subject_edition=1,
            subject_status="approved",
            ruleset_digest=preflight.digests.ruleset_digest,
            requirement_anchors=(),
            submission_fence=ValidationSubmissionFence(
                expected_validation_edition=1,
                expected_subject_version=7,
                expected_head_revision=self.head_revision,
            ),
        )

    async def resolve_assessment_preflight_request(
        self,
        request,
        *,
        actor_id,
        realm_scope,
    ):
        assert actor_id == "agent-1"
        assert realm_scope == RealmScope.local()
        assert request.expected_head_revision == self.head_revision
        return self.assessment_preflight()

    async def lookup_assessment_replay(self, **kwargs):
        assert kwargs["board_id"] == "b1"
        assert kwargs["idempotency_key"] == "lint-idem-1"
        assert kwargs["actor_id"] == "agent-1"
        assert kwargs["realm_scope"] == RealmScope.local()
        self.lookup_calls += 1
        return self.replay


class _RequirementLintReplayPersistence(_Persistence):
    def __init__(self, reader: _RequirementLintReplayReader) -> None:
        super().__init__()
        self.reader = reader

    async def apply_bundle_cas(self, bundle):
        self.bundles.append(bundle)
        result = AssessmentCommitResult(
            board_id=bundle.receipt.subject.board_id,
            subject_type=bundle.receipt.subject.subject_type,
            subject_id=bundle.receipt.subject.subject_id,
            subject_version=bundle.receipt.subject.subject_version,
            subject_edition=bundle.receipt.subject.subject_edition,
            assessment_kind=bundle.receipt.assessment_kind,
            request_fingerprint=bundle.request_fingerprint,
            receipt_id=bundle.receipt.id,
            head_revision=bundle.next_head.revision,
            event_id=bundle.audit_intent.event_id,
            history_id=bundle.audit_intent.history_id,
            outbox_id=bundle.audit_intent.outbox_id,
            qa_id_map=(),
        )
        self.reader.head_revision = result.head_revision
        self.reader.replay = replace(result, replayed=True)
        return result


@pytest.mark.asyncio
async def test_requirement_lint_exact_retry_replays_before_mutable_head_conflict(
) -> None:
    reader = _RequirementLintReplayReader()
    persistence = _RequirementLintReplayPersistence(reader)
    factory = _UowFactory(persistence)
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    use_case = RecordRequirementLintUseCase(
        preflight_reader=reader,
        uow_factory=factory,
        service=service,
    )
    command = RecordRequirementLintCommand(
        board_id="b1",
        spec_id="spec-1",
        expected_subject_version=7,
        expected_subject_edition=1,
        expected_head_revision=0,
        ruleset_digest=_digests().ruleset_digest,
        idempotency_key="lint-idem-1",
        score=0,
        summary="No requirement lint findings.",
    )
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id="b1",
        realm_scope=RealmScope.local(),
    )

    first = await use_case.execute(command, actor=actor)
    replay = await use_case.execute(command, actor=actor)

    assert first.replayed is False
    assert replay.replayed is True
    assert replay.receipt_id == first.receipt_id
    assert reader.head_revision == 1
    assert reader.lookup_calls == 2
    assert factory.opens == 1
    assert len(persistence.bundles) == 1


@pytest.mark.asyncio
async def test_use_case_opens_write_uow_only_after_all_prechecks_pass() -> None:
    persistence = _Persistence()
    factory = _UowFactory(persistence)
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=_PreflightReader(_preflight()),
        uow_factory=factory,
        service=QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW),
    )

    result = await use_case.execute(
        SubmitQualityAssessmentCommand(_submission()),
        actor=ActorContext(
            "agent-1",
            "mcp",
            board_id="b1",
            realm_scope=RealmScope.local(),
        ),
    )

    assert factory.opens == 1
    assert factory.uow.commits == 1
    assert result.head_revision == 4


@pytest.mark.asyncio
async def test_denial_never_opens_write_uow() -> None:
    factory = _UowFactory(_Persistence())
    denied = _preflight(
        authority=AssessmentAuthoritySnapshot(
            domain_write=False,
            quality_assess=True,
            authority_digest=AUTHORITY_DIGEST,
        )
    )
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=_PreflightReader(denied),
        uow_factory=factory,
        service=QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW),
    )

    with pytest.raises(QualityAssessmentForbiddenError):
        await use_case.execute(
            SubmitQualityAssessmentCommand(_submission()),
            actor=ActorContext(
                "agent-1",
                "rest",
                board_id="b1",
                realm_scope=RealmScope.local(),
            ),
        )
    assert factory.opens == 0


@pytest.mark.asyncio
async def test_actor_board_mismatch_fails_before_preflight_or_uow() -> None:
    reader = _PreflightReader(_preflight())
    factory = _UowFactory(_Persistence())
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=reader,
        uow_factory=factory,
    )

    with pytest.raises(QualityAssessmentForbiddenError) as exc_info:
        await use_case.execute(
            SubmitQualityAssessmentCommand(_submission()),
            actor=ActorContext(
                "agent-1",
                "mcp",
                board_id="other-board",
                realm_scope=RealmScope.local(),
            ),
        )

    assert exc_info.value.code == "assessment_board_scope_mismatch"
    assert reader.calls == 0
    assert factory.opens == 0


@pytest.mark.asyncio
async def test_missing_realm_fails_before_preflight_even_on_replay() -> None:
    reader = _PreflightReader(_preflight(), replay=object())
    factory = _UowFactory(_Persistence())
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=reader,
        uow_factory=factory,
    )

    with pytest.raises(MissingRealmScope, match="realm_scope_required"):
        await use_case.execute(
            SubmitQualityAssessmentCommand(_submission()),
            actor=ActorContext("agent-1", "mcp", board_id="b1"),
        )

    assert reader.calls == 0
    assert factory.opens == 0


@pytest.mark.asyncio
async def test_replay_returns_original_ids_after_subject_and_head_drift() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    submission = _submission()
    result = AssessmentCommitResult(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        subject_version=7,
        assessment_kind=AssessmentKind.AMBIGUITY,
        request_fingerprint=service.submission_fingerprint(
            submission,
            actor_id="agent-1",
        ),
        receipt_id="qar_original",
        head_revision=4,
        event_id="qae_original",
        history_id="qah_original",
        outbox_id="qao_original",
        qa_id_map=(),
        replayed=True,
    )
    drifted = replace(
        _preflight(),
        subject=replace(_preflight().subject, subject_version=8),
        current_head_revision=9,
        status="done",
        digests=replace(
            _preflight().digests,
            content_digest=canonical_sha256("new-content"),
            input_digest=None,
        ),
    )
    factory = _UowFactory(_Persistence())
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=_PreflightReader(drifted, replay=result),
        uow_factory=factory,
        service=service,
    )

    replay = await use_case.execute(
        SubmitQualityAssessmentCommand(submission),
        actor=ActorContext(
            "agent-1",
            "mcp",
            board_id="b1",
            realm_scope=RealmScope.local(),
        ),
    )

    assert replay.receipt_id == "qar_original"
    assert replay.replayed is True
    assert factory.opens == 0


def test_same_idempotency_key_with_different_payload_is_non_retryable_conflict() -> None:
    service = QualityAssessmentService()
    submission = _submission()
    result = AssessmentCommitResult(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        subject_version=7,
        assessment_kind=AssessmentKind.AMBIGUITY,
        request_fingerprint=canonical_sha256("different"),
        receipt_id="qar_original",
        head_revision=4,
        event_id="qae_original",
        history_id="qah_original",
        outbox_id="qao_original",
        qa_id_map=(),
        replayed=True,
    )
    with pytest.raises(QualityAssessmentConflictError) as exc_info:
        service.resolve_replay(
            submission,
            actor_id="agent-1",
            result=result,
        )
    assert exc_info.value.code == "assessment_idempotency_conflict"
    assert exc_info.value.retryable is False


def test_replay_lookup_result_must_be_marked_replayed() -> None:
    service = QualityAssessmentService()
    submission = _submission()
    result = AssessmentCommitResult(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        subject_version=7,
        assessment_kind=AssessmentKind.AMBIGUITY,
        request_fingerprint=service.submission_fingerprint(
            submission,
            actor_id="agent-1",
        ),
        receipt_id="qar_original",
        head_revision=4,
        event_id="qae_original",
        history_id="qah_original",
        outbox_id="qao_original",
        qa_id_map=(),
        replayed=False,
    )
    with pytest.raises(QualityAssessmentPortContractError) as exc_info:
        service.resolve_replay(
            submission,
            actor_id="agent-1",
            result=result,
        )
    assert exc_info.value.code == "assessment_replay_flag_invalid"


@pytest.mark.asyncio
async def test_question_budget_is_rejected_before_preflight_or_uow() -> None:
    questions = tuple(
        ProposedQuestionDraft(
            client_key=f"q{i}",
            question=f"Question {i}?",
            question_type="free_text",
        )
        for i in range(6)
    )
    reader = _PreflightReader(_preflight())
    factory = _UowFactory(_Persistence())
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=reader,
        uow_factory=factory,
    )
    with pytest.raises(QualityAssessmentValidationError) as exc_info:
        await use_case.execute(
            SubmitQualityAssessmentCommand(
                _submission(questions=questions)
            ),
            actor=ActorContext(
                "agent-1",
                "rest",
                board_id="b1",
                realm_scope=RealmScope.local(),
            ),
        )
    assert exc_info.value.code == "question_budget_exceeded"
    assert reader.calls == 0
    assert factory.opens == 0


@pytest.mark.asyncio
async def test_invalid_preflight_port_result_fails_closed_before_uow() -> None:
    reader = _PreflightReader(object())
    factory = _UowFactory(_Persistence())
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=reader,
        uow_factory=factory,
    )

    with pytest.raises(QualityAssessmentPortContractError) as exc_info:
        await use_case.execute(
            SubmitQualityAssessmentCommand(_submission()),
            actor=ActorContext(
                "agent-1",
                "mcp",
                board_id="b1",
                realm_scope=RealmScope.local(),
            ),
        )

    assert exc_info.value.code == "assessment_preflight_result_invalid"
    assert factory.opens == 0


@pytest.mark.asyncio
async def test_invalid_replay_port_result_fails_closed_without_attribute_error() -> None:
    reader = _PreflightReader(_preflight(), replay={"receipt_id": "invalid"})
    factory = _UowFactory(_Persistence())
    use_case = SubmitQualityAssessmentUseCase(
        preflight_reader=reader,
        uow_factory=factory,
    )

    with pytest.raises(QualityAssessmentPortContractError) as exc_info:
        await use_case.execute(
            SubmitQualityAssessmentCommand(_submission()),
            actor=ActorContext(
                "agent-1",
                "mcp",
                board_id="b1",
                realm_scope=RealmScope.local(),
            ),
        )

    assert exc_info.value.code == "assessment_commit_result_invalid"
    assert factory.opens == 0


@pytest.mark.asyncio
async def test_divergent_adapter_result_fails_closed() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    persistence = _Persistence()
    persistence.result = AssessmentCommitResult(
        board_id="other",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        subject_version=7,
        assessment_kind=AssessmentKind.AMBIGUITY,
        request_fingerprint=bundle.request_fingerprint,
        receipt_id=bundle.receipt.id,
        head_revision=4,
        event_id=bundle.audit_intent.event_id,
        history_id=bundle.audit_intent.history_id,
        outbox_id=bundle.audit_intent.outbox_id,
        qa_id_map=(),
    )
    with pytest.raises(QualityAssessmentPortContractError) as exc_info:
        await service.commit_prepared(bundle, persistence=persistence)
    assert exc_info.value.code == "assessment_commit_identity_mismatch"


@pytest.mark.asyncio
async def test_commit_result_must_echo_atomic_audit_identities() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    persistence = _Persistence()
    persistence.result = AssessmentCommitResult(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        subject_version=7,
        assessment_kind=AssessmentKind.AMBIGUITY,
        request_fingerprint=bundle.request_fingerprint,
        receipt_id=bundle.receipt.id,
        head_revision=4,
        event_id="wrong-event",
        history_id=bundle.audit_intent.history_id,
        outbox_id=bundle.audit_intent.outbox_id,
        qa_id_map=(),
    )
    with pytest.raises(QualityAssessmentPortContractError) as exc_info:
        await service.commit_prepared(bundle, persistence=persistence)
    assert exc_info.value.code == "assessment_commit_audit_identity_mismatch"


@pytest.mark.asyncio
async def test_invalid_current_and_receipt_port_results_fail_closed() -> None:
    class _InvalidReads(_Persistence):
        async def get_current(self, **kwargs):
            return ("not-a-receipt", "not-a-head")

        async def get_receipt(self, **kwargs):
            return {"id": "qar_1"}

    service = QualityAssessmentService()
    persistence = _InvalidReads()
    with pytest.raises(QualityAssessmentPortContractError) as current_error:
        await service.get_current(
            board_id="b1",
            subject_type=AssessmentSubjectType.REFINEMENT,
            subject_id="r1",
            assessment_kind=AssessmentKind.AMBIGUITY,
            current_subject=_preflight().subject,
            current_digests=_digests(),
            persistence=persistence,
        )
    assert current_error.value.code == "assessment_current_port_invalid"

    with pytest.raises(QualityAssessmentPortContractError) as receipt_error:
        await service.get_receipt(
            board_id="b1",
            receipt_id="qar_1",
            persistence=persistence,
        )
    assert receipt_error.value.code == "assessment_receipt_port_invalid"


@pytest.mark.asyncio
async def test_receipt_detail_preserves_finding_question_links() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(
            questions=(
                ProposedQuestionDraft(
                    client_key="q1",
                    question="What is observable?",
                    question_type="free_text",
                    finding_keys=("f1",),
                ),
            )
        ),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    detail = AssessmentReceiptDetail(
        receipt=bundle.receipt,
        findings=bundle.findings,
        proposed_questions=bundle.proposed_questions,
        finding_qa_links=bundle.finding_qa_links,
    )

    class _Detail(_Persistence):
        async def get_receipt_detail(self, **kwargs):
            return detail

    resolved = await service.get_receipt_detail(
        board_id="b1",
        receipt_id=bundle.receipt.id,
        persistence=_Detail(),
    )
    assert resolved.finding_qa_links[0].finding_id == resolved.findings[0].id
    assert (
        resolved.finding_qa_links[0].qa_id
        == resolved.proposed_questions[0].qa_id
    )


def test_assessment_recorded_event_is_versioned_and_content_free() -> None:
    event = QualityAssessmentRecorded(
        board_id="b1",
        actor_id="agent-1",
        subject_type="refinement",
        subject_id="r1",
        subject_version=7,
        assessment_kind="ambiguity",
        receipt_id="qar_1",
        input_digest=canonical_sha256("input"),
        request_fingerprint=canonical_sha256("request"),
        authority_digest=AUTHORITY_DIGEST,
        head_revision=4,
    )
    payload = event.payload_for_storage()
    assert payload["event_schema_version"] == 1
    assert payload["receipt_id"] == "qar_1"
    assert payload["authority_digest"] == AUTHORITY_DIGEST
    assert not {"content", "findings", "questions"} & payload.keys()


def test_transaction_service_catalog_exposes_registered_assessment_port() -> None:
    marker = object()

    class _Adapter:
        def quality_assessments(self, session):
            assert session == "session"
            return marker

    register_relational_application_adapter(_Adapter())
    try:
        catalog = build_application_service_catalog("session")
        assert catalog.quality_assessments is marker
    finally:
        reset_relational_application_adapter_for_tests()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("offset", "limit"),
    [
        (-1, 25),
        (True, 25),
        (0, 0),
        (0, 201),
        (0, True),
    ],
)
async def test_page_contract_rejects_invalid_windows(offset, limit) -> None:
    service = QualityAssessmentService()
    query = AssessmentListQuery(
        subject=_preflight().subject.identity,
        offset=offset,
        limit=limit,
    )
    with pytest.raises(QualityAssessmentValidationError) as exc_info:
        await service.list_assessments(query, persistence=_Persistence())
    assert exc_info.value.code == "invalid_pagination"


@pytest.mark.asyncio
async def test_list_contract_preserves_exact_two_totals_from_adapter() -> None:
    class _Paged(_Persistence):
        async def list_findings(self, query):
            return QualityPage((), 7, 19, query.offset, query.limit)

    page = await QualityAssessmentService().list_findings(
        FindingListQuery(
            board_id="b1",
            subject_type=AssessmentSubjectType.REFINEMENT,
            subject_id="r1",
            offset=25,
            limit=25,
        ),
        persistence=_Paged(),
    )
    assert page.total_filtered == 7
    assert page.total_overall == 19
    assert page.offset == 25


@pytest.mark.asyncio
async def test_assessment_list_recomputes_currentness_and_rejects_adapter_drift() -> None:
    service = QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)
    bundle = service.prepare_submission(
        _submission(),
        actor_id="agent-1",
        preflight=_preflight(),
    )
    wrong_digests = replace(
        bundle.receipt.digests,
        content_digest=canonical_sha256("wrong"),
        input_digest=None,
    )
    wrong_view = project_assessment_receipt_view(
        bundle.receipt,
        head_receipt_id=bundle.receipt.id,
        current_subject=bundle.receipt.subject,
        current_digests=wrong_digests,
    )

    class _Paged(_Persistence):
        async def list_assessments(self, query):
            return QualityPage((wrong_view,), 1, 1, query.offset, query.limit)

    with pytest.raises(QualityAssessmentPortContractError) as exc_info:
        await service.list_assessments(
            AssessmentListQuery(
                subject=bundle.receipt.subject.identity,
                offset=0,
                limit=25,
                current_subject_version=bundle.receipt.subject.subject_version,
                current_digests=bundle.receipt.digests,
            ),
            persistence=_Paged(),
        )
    assert exc_info.value.code == "assessment_page_scope_mismatch"


@pytest.mark.asyncio
async def test_assessment_list_rejects_untyped_items_before_sorting() -> None:
    class _Paged(_Persistence):
        async def list_assessments(self, query):
            return QualityPage((object(),), 1, 1, query.offset, query.limit)

    with pytest.raises(QualityAssessmentPortContractError) as exc_info:
        await QualityAssessmentService().list_assessments(
            AssessmentListQuery(
                subject=_preflight().subject.identity,
                offset=0,
                limit=25,
                current_subject_version=7,
                current_digests=_digests(),
            ),
            persistence=_Paged(),
        )
    assert exc_info.value.code == "assessment_page_item_invalid"
