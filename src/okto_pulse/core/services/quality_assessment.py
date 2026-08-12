"""Core orchestration for SK-A quality assessments.

The service has an explicit two-step API.  ``prepare_submission`` performs all
payload, authority, status, version, question-budget, and anchor checks without
touching a writable persistence port.  Only a successfully prepared immutable
bundle may be handed to ``commit_prepared``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

from okto_pulse.core.domain.quality_assessment import (
    MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1,
    AssessmentAuditIntent,
    AssessmentCommitResult,
    AssessmentCurrentness,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOutcome,
    AssessmentOrigin,
    AssessmentPreflight,
    AssessmentReceipt,
    AssessmentReceiptDetail,
    AssessmentReceiptState,
    AssessmentReceiptView,
    AssessmentScaleKind,
    AssessmentSubjectHead,
    AssessmentSubjectIdentity,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentSubmission,
    AssessmentSource,
    AssessmentWriteBundle,
    FindingAnchorType,
    FindingLifecycle,
    FindingQaLink,
    FindingSeverity,
    ProposedQuestion,
    QualityAssessmentContractError,
    QualityFinding,
    QualityPage,
    QualityPageCursor,
    ScoreDirection,
    evaluate_assessment_currentness,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.ports.application_persistence import PAGE_LIMIT_MAX, PAGE_OFFSET_MAX
from okto_pulse.core.ports.quality_assessment import (
    AssessmentAuthorityConflict,
    AssessmentHeadRevisionConflict,
    AssessmentIdempotencyConflict,
    AssessmentInputDigestConflict,
    AssessmentListQuery,
    AssessmentCurrentnessInput,
    AssessmentSubjectStatusConflict,
    AssessmentSubjectLifecycleConflict,
    AssessmentSubjectEditionConflict,
    AssessmentSubjectVersionConflict,
    FindingListQuery,
    QualityGateInput,
    QualityAssessmentPersistencePort,
)
from okto_pulse.core.services.ambiguity_assessment import (
    AmbiguityGateReason,
)


class QualityAssessmentErrorCategory(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    UNPROCESSABLE = "unprocessable"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    INTERNAL = "internal"


class QualityAssessmentError(RuntimeError):
    """Transport-neutral structured SK-A failure."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        category: QualityAssessmentErrorCategory,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message or code
        self.category = category
        self.retryable = retryable
        self.details = dict(details or {})
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": self.message,
            "category": self.category.value,
            "retryable": self.retryable,
            "details": dict(self.details),
        }


class QualityAssessmentValidationError(QualityAssessmentError):
    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
        category: QualityAssessmentErrorCategory = (
            QualityAssessmentErrorCategory.UNPROCESSABLE
        ),
    ) -> None:
        super().__init__(
            code,
            message,
            category=category,
            retryable=False,
            details=details,
        )


class QualityAssessmentForbiddenError(QualityAssessmentError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(
            code,
            message,
            category=QualityAssessmentErrorCategory.FORBIDDEN,
            retryable=False,
        )


class QualityAssessmentConflictError(QualityAssessmentError):
    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        retryable: bool = True,
    ) -> None:
        super().__init__(
            code,
            message,
            category=QualityAssessmentErrorCategory.CONFLICT,
            retryable=retryable,
        )


class QualityAssessmentNotFoundError(QualityAssessmentError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(
            code,
            message,
            category=QualityAssessmentErrorCategory.NOT_FOUND,
            retryable=False,
        )


class QualityAssessmentPortContractError(QualityAssessmentError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(
            code,
            message,
            category=QualityAssessmentErrorCategory.INTERNAL,
            retryable=False,
        )


@dataclass(frozen=True, slots=True)
class CurrentAssessmentView:
    receipt: AssessmentReceipt
    head: AssessmentSubjectHead
    currentness: AssessmentCurrentness
    gate_preview: "QualityGatePreview"


@dataclass(frozen=True, slots=True)
class QualityGatePreview:
    """Closed, transport-neutral preview of the gate affected by a receipt."""

    applicable: bool
    enabled: bool
    allowed: bool
    reason_code: str
    threshold: float | None
    score: float
    skipped: bool

    def __post_init__(self) -> None:
        for field_name in ("applicable", "enabled", "allowed", "skipped"):
            if not isinstance(getattr(self, field_name), bool):
                raise QualityAssessmentPortContractError(
                    f"quality_gate_preview_{field_name}_invalid"
                )
        if not isinstance(self.reason_code, str) or not self.reason_code:
            raise QualityAssessmentPortContractError(
                "quality_gate_preview_reason_code_invalid"
            )
        if self.threshold is not None and (
            not isinstance(self.threshold, int | float)
            or isinstance(self.threshold, bool)
        ):
            raise QualityAssessmentPortContractError(
                "quality_gate_preview_threshold_invalid"
            )
        if not isinstance(self.score, int | float) or isinstance(self.score, bool):
            raise QualityAssessmentPortContractError(
                "quality_gate_preview_score_invalid"
            )


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _strict_page_window(offset: int, limit: int) -> tuple[int, int]:
    if (
        not isinstance(offset, int)
        or isinstance(offset, bool)
        or offset < 0
        or offset > PAGE_OFFSET_MAX
        or not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= PAGE_LIMIT_MAX
    ):
        raise QualityAssessmentValidationError(
            "invalid_pagination",
            f"offset must be 0..{PAGE_OFFSET_MAX} and limit must be 1..{PAGE_LIMIT_MAX}",
            category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
        )
    return offset, limit


class QualityAssessmentService:
    """Validate, prepare, commit, and read assessment aggregates."""

    def __init__(
        self,
        *,
        id_factory: Callable[[str], str] = _default_id_factory,
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self._id_factory = id_factory
        self._clock = clock

    def validate_submission_envelope(
        self,
        submission: AssessmentSubmission,
        *,
        actor_id: str,
    ) -> str:
        """Validate immutable inbound data before any external read or UoW."""

        if not isinstance(submission, AssessmentSubmission):
            raise QualityAssessmentValidationError(
                "assessment_submission_invalid"
            )
        if not isinstance(actor_id, str) or not actor_id.strip():
            raise QualityAssessmentValidationError("assessment_actor_required")
        actor = actor_id.strip()
        if len(submission.proposed_questions) > MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1:
            raise QualityAssessmentValidationError(
                "question_budget_exceeded",
                "An assessment may propose at most five questions.",
                details={
                    "maximum": MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1,
                    "actual": len(submission.proposed_questions),
                },
            )
        finding_keys = [item.finding_key for item in submission.findings]
        if len(finding_keys) != len(set(finding_keys)):
            raise QualityAssessmentValidationError("duplicate_finding_key")
        client_keys = [item.client_key for item in submission.proposed_questions]
        if len(client_keys) != len(set(client_keys)):
            raise QualityAssessmentValidationError("duplicate_client_key")
        if (
            submission.score != submission.scale.best_score
            and not submission.findings
        ):
            raise QualityAssessmentValidationError(
                "assessment_finding_required"
            )
        valid_keys = set(finding_keys)
        for question in submission.proposed_questions:
            if any(key not in valid_keys for key in question.finding_keys):
                raise QualityAssessmentValidationError(
                    "question_finding_key_not_found"
                )
        return actor

    def submission_fingerprint(
        self,
        submission: AssessmentSubmission,
        *,
        actor_id: str,
    ) -> str:
        """Hash only immutable inbound command data for replay lookup."""

        actor = self.validate_submission_envelope(
            submission,
            actor_id=actor_id,
        )
        return canonical_sha256(
            {
                "actor_id": actor,
                "board_id": submission.board_id,
                "subject_type": submission.subject_type,
                "subject_id": submission.subject_id,
                "assessment_kind": submission.assessment_kind,
                "idempotency_key": submission.idempotency_key,
                "expected_subject_version": submission.expected_subject_version,
                "expected_subject_edition": submission.expected_subject_edition,
                "expected_head_revision": submission.expected_head_revision,
                "score": submission.score,
                "justification": submission.justification,
                "scale": asdict(submission.scale),
                # List order remains semantic under canonical-json/v1.
                "findings": [asdict(item) for item in submission.findings],
                "proposed_questions": [
                    asdict(item) for item in submission.proposed_questions
                ],
            }
        )

    def validate_replay_authority(
        self,
        submission: AssessmentSubmission,
        *,
        preflight: AssessmentPreflight,
    ) -> None:
        """Authorize a replay without applying mutable status/version fences."""

        subject = preflight.subject
        if (
            submission.board_id != subject.board_id
            or submission.subject_type is not subject.subject_type
            or submission.subject_id != subject.subject_id
        ):
            raise QualityAssessmentValidationError("assessment_subject_mismatch")
        authority = preflight.authority
        if (
            submission.subject_type is AssessmentSubjectType.SPEC
            and submission.assessment_kind is AssessmentKind.REQUIREMENT_LINT
            and preflight.origin is AssessmentOrigin.SEMANTIC_WRITER
            and preflight.subject.subject_edition is None
        ):
            if not authority.domain_write:
                raise QualityAssessmentForbiddenError(
                    "assessment_permission_denied",
                    "Automatic requirement lint inherits the semantic writer's domain authority.",
                )
            return
        if not authority.domain_write or not authority.quality_assess:
            raise QualityAssessmentForbiddenError(
                "assessment_permission_denied",
                "The domain write authority and granular quality-assess leaf are both required.",
            )
        if (
            submission.subject_type is AssessmentSubjectType.SPEC
            and submission.assessment_kind is AssessmentKind.REQUIREMENT_LINT
            and preflight.subject.subject_edition is not None
        ):
            # External lint is advisory evidence, not submission of the Spec
            # Validation gate. It must not inherit that gate's permission or
            # reviewer-separation requirements.
            return
        # Proposed questions are immutable assessment evidence. They do not
        # create or mutate the subject's Q&A collection, so qa_ask authority is
        # deliberately not required here.
        if submission.subject_type is AssessmentSubjectType.SPEC:
            if not preflight.authority.spec_validation_submit:
                raise QualityAssessmentForbiddenError(
                    "spec_assessment_requires_validation"
                )
            if not authority.reviewer_separation_satisfied:
                raise QualityAssessmentForbiddenError(
                    "spec_reviewer_separation_required"
                )

    def resolve_replay(
        self,
        submission: AssessmentSubmission,
        *,
        actor_id: str,
        result: AssessmentCommitResult,
    ) -> AssessmentCommitResult:
        """Validate a read-side idempotent result before returning it."""

        if not isinstance(result, AssessmentCommitResult):
            raise QualityAssessmentPortContractError(
                "assessment_commit_result_invalid"
            )
        if not result.replayed:
            raise QualityAssessmentPortContractError(
                "assessment_replay_flag_invalid"
            )
        fingerprint = self.submission_fingerprint(
            submission,
            actor_id=actor_id,
        )
        if result.request_fingerprint != fingerprint:
            raise QualityAssessmentConflictError(
                "assessment_idempotency_conflict",
                retryable=False,
            )
        self._validate_commit_result(
            submission=submission,
            fingerprint=fingerprint,
            result=result,
            bundle=None,
        )
        return result

    def prepare_submission(
        self,
        submission: AssessmentSubmission,
        *,
        actor_id: str,
        preflight: AssessmentPreflight,
    ) -> AssessmentWriteBundle:
        """Purely prepare an immutable bundle; no persistence port is touched."""

        try:
            return self._prepare_submission(
                submission,
                actor_id=actor_id,
                preflight=preflight,
            )
        except QualityAssessmentError:
            raise
        except QualityAssessmentContractError as exc:
            raise QualityAssessmentValidationError(exc.code, exc.message) from exc

    def _prepare_submission(
        self,
        submission: AssessmentSubmission,
        *,
        actor_id: str,
        preflight: AssessmentPreflight,
    ) -> AssessmentWriteBundle:
        actor = self.validate_submission_envelope(
            submission,
            actor_id=actor_id,
        )

        self._validate_subject_fence(submission, preflight)
        self._validate_authority_and_status(submission, preflight)
        self._validate_findings_and_questions(submission, preflight)

        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise QualityAssessmentValidationError(
                "assessment_clock_must_be_timezone_aware"
            )
        now = now.astimezone(timezone.utc)

        request_fingerprint = self.submission_fingerprint(
            submission,
            actor_id=actor,
        )
        run_identity_digest = canonical_sha256(
            {
                "board_id": preflight.subject.board_id,
                "subject_type": preflight.subject.subject_type,
                "subject_id": preflight.subject.subject_id,
                "subject_version": preflight.subject.subject_version,
                "subject_edition": preflight.subject.subject_edition,
                "assessment_kind": submission.assessment_kind,
                "ruleset_version": preflight.versions.ruleset_version,
                "analyzer_version": preflight.versions.analyzer_version,
                "input_digest": preflight.digests.input_digest,
            }
        )
        receipt_id = self._id_factory("qar")
        receipt = AssessmentReceipt(
            id=receipt_id,
            subject=preflight.subject,
            assessment_kind=submission.assessment_kind,
            origin=preflight.origin,
            source=(
                AssessmentSource.LEGACY_MIGRATION
                if preflight.origin is AssessmentOrigin.LEGACY_IMPORT
                else AssessmentSource.NATIVE
            ),
            channel=preflight.channel,
            outcome=(
                AssessmentOutcome.ADVISORY
                if submission.assessment_kind is AssessmentKind.REQUIREMENT_LINT
                else AssessmentOutcome.RECORDED
            ),
            scale=submission.scale,
            score=submission.score,
            justification=submission.justification,
            digests=preflight.digests,
            versions=preflight.versions,
            run_identity_digest=run_identity_digest,
            authority_digest=preflight.authority.authority_digest,
            idempotency_key=submission.idempotency_key,
            request_digest=request_fingerprint,
            created_by=actor,
            created_at=now,
            predecessor_receipt_id=preflight.current_head_receipt_id,
        )

        findings: list[QualityFinding] = []
        finding_ids: dict[str, str] = {}
        for draft in submission.findings:
            finding_id = self._id_factory("qf")
            finding_ids[draft.finding_key] = finding_id
            findings.append(
                QualityFinding(
                    id=finding_id,
                    receipt_id=receipt_id,
                    assessment_kind=submission.assessment_kind,
                    finding_key=draft.finding_key,
                    category_code=draft.category_code,
                    taxonomy_version=preflight.versions.taxonomy_version,
                    severity=draft.severity,
                    confidence=draft.confidence,
                    deterministic=draft.deterministic,
                    blocking_eligible=draft.blocking_eligible,
                    title=draft.title,
                    detail=draft.detail,
                    anchor=draft.anchor,
                    evidence_refs=draft.evidence_refs,
                    lifecycle=FindingLifecycle.OPEN,
                    created_at=now,
                    remediation=draft.remediation,
                    rule_code=draft.rule_code,
                )
            )

        questions: list[ProposedQuestion] = []
        qa_ids: dict[str, str] = {}
        for draft in submission.proposed_questions:
            qa_id = self._id_factory("qa")
            qa_ids[draft.client_key] = qa_id
            questions.append(
                ProposedQuestion(
                    qa_id=qa_id,
                    receipt_id=receipt_id,
                    client_key=draft.client_key,
                    question=draft.question,
                    question_type=draft.question_type,
                    choices=draft.choices,
                    allow_free_text=draft.allow_free_text,
                    category_code=draft.category_code,
                    created_at=now,
                )
            )

        links = tuple(
            FindingQaLink(
                finding_id=finding_ids[finding_key],
                qa_id=qa_ids[question.client_key],
                receipt_id=receipt_id,
            )
            for question in submission.proposed_questions
            for finding_key in question.finding_keys
        )
        next_head = AssessmentSubjectHead(
            board_id=preflight.subject.board_id,
            subject_type=preflight.subject.subject_type,
            subject_id=preflight.subject.subject_id,
            assessment_kind=submission.assessment_kind,
            receipt_id=receipt_id,
            revision=submission.expected_head_revision + 1,
            updated_at=now,
        )
        audit_intent = AssessmentAuditIntent(
            event_id=self._id_factory("qae"),
            history_id=self._id_factory("qah"),
            outbox_id=self._id_factory("qao"),
            subject=preflight.subject,
            assessment_kind=submission.assessment_kind,
            receipt_id=receipt_id,
            input_digest=preflight.digests.input_digest or "",
            request_fingerprint=request_fingerprint,
            head_revision=next_head.revision,
            actor_id=actor,
            authority_digest=preflight.authority.authority_digest,
            occurred_at=now,
        )
        return AssessmentWriteBundle(
            idempotency_key=submission.idempotency_key,
            request_fingerprint=request_fingerprint,
            expected_subject_version=submission.expected_subject_version,
            expected_subject_edition=submission.expected_subject_edition,
            expected_head_revision=submission.expected_head_revision,
            expected_head_receipt_id=preflight.current_head_receipt_id,
            expected_subject_status=preflight.status,
            expected_subject_archived=preflight.subject_archived,
            expected_input_digest=preflight.digests.input_digest or "",
            expected_authority_digest=preflight.authority.authority_digest,
            receipt=receipt,
            findings=tuple(findings),
            proposed_questions=tuple(questions),
            finding_qa_links=links,
            next_head=next_head,
            audit_intent=audit_intent,
        )

    @staticmethod
    def _validate_subject_fence(
        submission: AssessmentSubmission,
        preflight: AssessmentPreflight,
    ) -> None:
        subject = preflight.subject
        if (
            submission.board_id != subject.board_id
            or submission.subject_type is not subject.subject_type
            or submission.subject_id != subject.subject_id
        ):
            raise QualityAssessmentValidationError("assessment_subject_mismatch")
        if (
            subject.subject_edition is not None
            and submission.expected_subject_edition != subject.subject_edition
        ):
            raise QualityAssessmentConflictError(
                "assessment_subject_edition_conflict"
            )
        if submission.expected_subject_version != subject.subject_version:
            raise QualityAssessmentConflictError(
                "assessment_subject_version_conflict"
            )
        if submission.expected_head_revision != preflight.current_head_revision:
            raise QualityAssessmentConflictError(
                "assessment_head_revision_conflict"
            )
        if submission.scale != preflight.expected_scale:
            raise QualityAssessmentValidationError(
                "assessment_scale_mismatch"
            )
        if preflight.subject_archived:
            raise QualityAssessmentConflictError(
                "assessment_subject_lifecycle_conflict",
                retryable=False,
            )

    def _validate_authority_and_status(
        self,
        submission: AssessmentSubmission,
        preflight: AssessmentPreflight,
    ) -> None:
        self.validate_replay_authority(
            submission,
            preflight=preflight,
        )
        authority = preflight.authority

        subject_type = submission.subject_type
        if subject_type is AssessmentSubjectType.IDEATION:
            if submission.assessment_kind is not AssessmentKind.AMBIGUITY:
                raise QualityAssessmentValidationError(
                    "assessment_kind_not_supported_for_subject"
                )
            if preflight.origin is not AssessmentOrigin.HUMAN_OR_AGENT:
                raise QualityAssessmentForbiddenError(
                    "assessment_origin_forbidden"
                )
            if preflight.status != "evaluating":
                raise QualityAssessmentConflictError(
                    "assessment_subject_status_conflict",
                    retryable=False,
                )
            return
        if subject_type is AssessmentSubjectType.REFINEMENT:
            if submission.assessment_kind is not AssessmentKind.AMBIGUITY:
                raise QualityAssessmentValidationError(
                    "assessment_kind_not_supported_for_subject"
                )
            if preflight.origin is not AssessmentOrigin.HUMAN_OR_AGENT:
                raise QualityAssessmentForbiddenError(
                    "assessment_origin_forbidden"
                )
            if preflight.status != "approved":
                raise QualityAssessmentConflictError(
                    "assessment_subject_status_conflict",
                    retryable=False,
                )
            return

        if submission.assessment_kind is AssessmentKind.REQUIREMENT_LINT:
            if preflight.subject.subject_edition is not None:
                if preflight.origin is not AssessmentOrigin.HUMAN_OR_AGENT:
                    raise QualityAssessmentForbiddenError(
                        "requirement_lint_internal_cognition_forbidden"
                    )
                if preflight.status != "approved":
                    raise QualityAssessmentConflictError(
                        "assessment_subject_status_conflict",
                        retryable=False,
                    )
                if (
                    submission.scale.kind
                    is not AssessmentScaleKind.FINDING_COUNT
                    or submission.scale.direction
                    is not ScoreDirection.LOWER_BETTER
                    or submission.scale.minimum != 0
                    or submission.score != float(len(submission.findings))
                    or submission.proposed_questions
                ):
                    raise QualityAssessmentValidationError(
                        "requirement_lint_external_contract_invalid"
                    )
            elif preflight.origin is not AssessmentOrigin.SEMANTIC_WRITER:
                raise QualityAssessmentForbiddenError(
                    "requirement_lint_external_submission_forbidden"
                )
            return
        if (
            preflight.origin is not AssessmentOrigin.SPEC_VALIDATION
            or not authority.spec_validation_submit
        ):
            raise QualityAssessmentForbiddenError(
                "spec_assessment_requires_validation"
            )
        if not authority.reviewer_separation_satisfied:
            raise QualityAssessmentForbiddenError(
                "spec_reviewer_separation_required"
            )

    @staticmethod
    def _validate_findings_and_questions(
        submission: AssessmentSubmission,
        preflight: AssessmentPreflight,
    ) -> None:
        subject = preflight.subject
        anchors = preflight.anchors
        for finding in submission.findings:
            if finding.category_code not in preflight.allowed_category_codes:
                raise QualityAssessmentValidationError(
                    "finding_category_code_unknown"
                )
            anchor = finding.anchor
            if (
                anchor.board_id != subject.board_id
                or anchor.subject_type is not subject.subject_type
                or anchor.subject_id != subject.subject_id
                or anchor.subject_version != subject.subject_version
                or anchor.input_digest != preflight.digests.input_digest
            ):
                raise QualityAssessmentValidationError("finding_anchor_mismatch")
            if (
                anchor.anchor_type is FindingAnchorType.FIELD
                and anchor.anchor_ref not in anchors.fields
            ):
                raise QualityAssessmentValidationError(
                    "finding_anchor_not_found"
                )
            if (
                anchor.anchor_type is FindingAnchorType.STRUCTURED_CHILD
                and anchor.anchor_ref not in anchors.structured_child_ids
            ):
                raise QualityAssessmentValidationError(
                    "finding_anchor_not_found"
                )
            if (
                anchor.anchor_type is FindingAnchorType.QA
                and anchor.anchor_ref not in anchors.qa_ids
            ):
                raise QualityAssessmentValidationError(
                    "finding_anchor_not_found"
                )
        for question in submission.proposed_questions:
            if (
                question.category_code is not None
                and question.category_code not in preflight.allowed_category_codes
            ):
                raise QualityAssessmentValidationError(
                    "question_category_code_unknown"
                )

    async def commit_prepared(
        self,
        bundle: AssessmentWriteBundle,
        *,
        persistence: QualityAssessmentPersistencePort,
    ) -> AssessmentCommitResult:
        """Apply a prepared bundle through the adapter's database CAS."""

        try:
            result = await persistence.apply_bundle_cas(bundle)
        except AssessmentHeadRevisionConflict as exc:
            raise QualityAssessmentConflictError(
                "assessment_head_revision_conflict"
            ) from exc
        except AssessmentSubjectVersionConflict as exc:
            raise QualityAssessmentConflictError(
                "assessment_subject_version_conflict"
            ) from exc
        except AssessmentSubjectEditionConflict as exc:
            raise QualityAssessmentConflictError(
                "assessment_subject_edition_conflict"
            ) from exc
        except AssessmentIdempotencyConflict as exc:
            raise QualityAssessmentConflictError(
                "assessment_idempotency_conflict",
                retryable=False,
            ) from exc
        except AssessmentInputDigestConflict as exc:
            raise QualityAssessmentConflictError(
                "assessment_input_digest_conflict"
            ) from exc
        except AssessmentSubjectStatusConflict as exc:
            raise QualityAssessmentConflictError(
                "assessment_subject_status_conflict"
            ) from exc
        except AssessmentSubjectLifecycleConflict as exc:
            raise QualityAssessmentConflictError(
                "assessment_subject_lifecycle_conflict",
                retryable=False,
            ) from exc
        except AssessmentAuthorityConflict as exc:
            raise QualityAssessmentForbiddenError(
                "assessment_authority_changed"
            ) from exc
        self._validate_commit_result(
            submission=None,
            fingerprint=bundle.request_fingerprint,
            result=result,
            bundle=bundle,
        )
        return result

    @staticmethod
    def _validate_commit_result(
        *,
        submission: AssessmentSubmission | None,
        fingerprint: str,
        result: AssessmentCommitResult,
        bundle: AssessmentWriteBundle | None,
    ) -> None:
        if not isinstance(result, AssessmentCommitResult):
            raise QualityAssessmentPortContractError(
                "assessment_commit_result_invalid"
            )
        if bundle is None and submission is None:
            raise QualityAssessmentPortContractError(
                "assessment_commit_validation_input_missing"
            )
        if result.request_fingerprint != fingerprint:
            raise QualityAssessmentPortContractError(
                "assessment_commit_fingerprint_mismatch"
            )
        source = bundle.receipt if bundle is not None else None
        if source is None:
            assert submission is not None
        expected_board = (
            source.subject.board_id if source is not None else submission.board_id
        )
        expected_type = (
            source.subject.subject_type if source is not None else submission.subject_type
        )
        expected_id = (
            source.subject.subject_id if source is not None else submission.subject_id
        )
        expected_version = (
            source.subject.subject_version
            if source is not None
            else submission.expected_subject_version
        )
        expected_edition = (
            source.subject.subject_edition
            if source is not None
            else submission.expected_subject_edition
        )
        expected_kind = (
            source.assessment_kind
            if source is not None
            else submission.assessment_kind
        )
        expected_head = (
            bundle.expected_head_revision + 1
            if bundle is not None
            else submission.expected_head_revision + 1
        )
        if (
            result.board_id != expected_board
            or result.subject_type is not expected_type
            or result.subject_id != expected_id
            or result.subject_version != expected_version
            or (
                expected_edition is not None
                and result.subject_edition != expected_edition
            )
            or result.assessment_kind is not expected_kind
            or result.head_revision != expected_head
        ):
            raise QualityAssessmentPortContractError(
                "assessment_commit_identity_mismatch"
            )
        expected_questions = (
            {
                item.client_key: item.qa_id
                for item in bundle.proposed_questions
            }
            if bundle is not None
            else {
                item.client_key: None
                for item in submission.proposed_questions
            }
        )
        actual_questions = dict(result.qa_id_map)
        if set(actual_questions) != set(expected_questions):
            raise QualityAssessmentPortContractError(
                "assessment_commit_qa_map_mismatch"
            )
        if len(set(actual_questions.values())) != len(actual_questions):
            raise QualityAssessmentPortContractError(
                "assessment_commit_qa_map_mismatch"
            )
        if bundle is not None and not result.replayed:
            if result.receipt_id != bundle.receipt.id:
                raise QualityAssessmentPortContractError(
                    "assessment_commit_receipt_mismatch"
                )
            if actual_questions != expected_questions:
                raise QualityAssessmentPortContractError(
                    "assessment_commit_qa_map_mismatch"
                )
            if (
                result.event_id != bundle.audit_intent.event_id
                or result.history_id != bundle.audit_intent.history_id
                or result.outbox_id != bundle.audit_intent.outbox_id
            ):
                raise QualityAssessmentPortContractError(
                    "assessment_commit_audit_identity_mismatch"
                )

    async def submit(
        self,
        submission: AssessmentSubmission,
        *,
        actor_id: str,
        preflight: AssessmentPreflight,
        persistence: QualityAssessmentPersistencePort,
    ) -> AssessmentCommitResult:
        """Convenience wrapper retaining prepare-before-write ordering."""

        bundle = self.prepare_submission(
            submission,
            actor_id=actor_id,
            preflight=preflight,
        )
        return await self.commit_prepared(bundle, persistence=persistence)

    async def get_current(
        self,
        *,
        board_id: str,
        subject_type: AssessmentSubjectType,
        subject_id: str,
        assessment_kind: AssessmentKind,
        current_subject: AssessmentSubjectRef,
        current_digests: AssessmentDigestSet | None = None,
        currentness_inputs: tuple[AssessmentCurrentnessInput, ...] = (),
        gate_inputs: tuple[QualityGateInput, ...] = (),
        persistence: QualityAssessmentPersistencePort,
    ) -> CurrentAssessmentView:
        try:
            resolved = await persistence.get_current(
                board_id=board_id,
                subject_type=subject_type,
                subject_id=subject_id,
                assessment_kind=assessment_kind,
                subject_edition=current_subject.subject_edition,
            )
        except TypeError as exc:
            # Transitional compatibility for third-party adapters compiled
            # against the pre-edition optional keyword.
            if "subject_edition" not in str(exc):
                raise
            resolved = await persistence.get_current(
                board_id=board_id,
                subject_type=subject_type,
                subject_id=subject_id,
                assessment_kind=assessment_kind,
            )
        if resolved is None:
            raise QualityAssessmentNotFoundError(
                "assessment_current_not_found"
            )
        if (
            not isinstance(resolved, tuple | list)
            or len(resolved) != 2
            or not isinstance(resolved[0], AssessmentReceipt)
            or not isinstance(resolved[1], AssessmentSubjectHead)
        ):
            raise QualityAssessmentPortContractError(
                "assessment_current_port_invalid"
            )
        receipt, head = resolved
        if receipt.subject.subject_edition != current_subject.subject_edition:
            # Adapters predating the optional selector may still return a global
            # head. It is history, never current for this lifecycle edition.
            raise QualityAssessmentNotFoundError(
                "assessment_current_not_found"
            )
        if (
            receipt.id != head.receipt_id
            or receipt.subject.board_id != board_id
            or receipt.subject.subject_type is not subject_type
            or receipt.subject.subject_id != subject_id
            or receipt.assessment_kind is not assessment_kind
            or head.board_id != board_id
            or head.subject_type is not subject_type
            or head.subject_id != subject_id
            or head.assessment_kind is not assessment_kind
        ):
            raise QualityAssessmentPortContractError(
                "assessment_current_port_mismatch"
            )
        currentness = evaluate_assessment_currentness(
            receipt,
            current_subject=current_subject,
            current_digests=(
                receipt.digests
                if current_subject.subject_edition is not None
                else _current_digests_for_receipt(
                    receipt,
                    inputs=currentness_inputs,
                    fallback=current_digests,
                )
            ),
        )
        return CurrentAssessmentView(
            receipt=receipt,
            head=head,
            currentness=currentness,
            gate_preview=_quality_gate_preview(
                receipt,
                currentness=currentness,
                gate_inputs=gate_inputs,
            ),
        )

    def currentness_for_receipt(
        self,
        receipt: AssessmentReceipt,
        *,
        current_subject: AssessmentSubjectRef,
        currentness_inputs: tuple[AssessmentCurrentnessInput, ...],
    ) -> AssessmentCurrentness:
        """Evaluate one receipt through the closed identity selector."""

        if not isinstance(receipt, AssessmentReceipt) or not isinstance(
            current_subject,
            AssessmentSubjectRef,
        ):
            raise QualityAssessmentValidationError(
                "assessment_currentness_inputs_required",
                category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
            )
        return evaluate_assessment_currentness(
            receipt,
            current_subject=current_subject,
            current_digests=(
                receipt.digests
                if current_subject.subject_edition is not None
                else _current_digests_for_receipt(
                    receipt,
                    inputs=currentness_inputs,
                    fallback=None,
                )
            ),
        )

    async def get_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
        persistence: QualityAssessmentPersistencePort,
    ) -> AssessmentReceipt:
        receipt = await persistence.get_receipt(
            board_id=board_id,
            receipt_id=receipt_id,
        )
        if receipt is None:
            raise QualityAssessmentNotFoundError("assessment_receipt_not_found")
        if not isinstance(receipt, AssessmentReceipt):
            raise QualityAssessmentPortContractError(
                "assessment_receipt_port_invalid"
            )
        if receipt.id != receipt_id or receipt.subject.board_id != board_id:
            raise QualityAssessmentPortContractError(
                "assessment_receipt_port_mismatch"
            )
        return receipt

    async def get_receipt_detail(
        self,
        *,
        board_id: str,
        receipt_id: str,
        persistence: QualityAssessmentPersistencePort,
    ) -> AssessmentReceiptDetail:
        detail = await persistence.get_receipt_detail(
            board_id=board_id,
            receipt_id=receipt_id,
        )
        if detail is None:
            raise QualityAssessmentNotFoundError("assessment_receipt_not_found")
        if not isinstance(detail, AssessmentReceiptDetail):
            raise QualityAssessmentPortContractError(
                "assessment_receipt_detail_port_invalid"
            )
        if (
            detail.receipt.id != receipt_id
            or detail.receipt.subject.board_id != board_id
        ):
            raise QualityAssessmentPortContractError(
                "assessment_receipt_detail_port_mismatch"
            )
        return detail

    async def list_assessments(
        self,
        query: AssessmentListQuery,
        *,
        persistence: QualityAssessmentPersistencePort,
    ) -> QualityPage[AssessmentReceiptView]:
        if (
            not isinstance(query, AssessmentListQuery)
            or not isinstance(query.subject, AssessmentSubjectIdentity)
            or (
                query.assessment_kind is not None
                and not isinstance(query.assessment_kind, AssessmentKind)
            )
        ):
            raise QualityAssessmentValidationError(
                "assessment_list_query_invalid",
                category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
            )
        _strict_page_window(query.offset, query.limit)
        if query.cursor is not None and (
            not isinstance(query.cursor, QualityPageCursor)
            or query.cursor.offset != query.offset
        ):
            raise QualityAssessmentValidationError(
                "assessment_cursor_invalid",
                category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
            )
        if query.state is not None and not isinstance(
            query.state,
            AssessmentReceiptState,
        ):
            raise QualityAssessmentValidationError(
                "assessment_state_filter_invalid",
                category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
            )
        has_identity_inputs = bool(query.currentness_inputs) and all(
            isinstance(item, AssessmentCurrentnessInput)
            for item in query.currentness_inputs
        )
        if (
            not isinstance(query.current_subject_version, int)
            or isinstance(query.current_subject_version, bool)
            or query.current_subject_version < 1
            or (
                query.current_subject_edition is None
                and
                not has_identity_inputs
                and not isinstance(query.current_digests, AssessmentDigestSet)
            )
        ):
            raise QualityAssessmentValidationError(
                "assessment_currentness_inputs_required",
                category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
            )
        page = await persistence.list_assessments(query)
        if not isinstance(page, QualityPage):
            raise QualityAssessmentPortContractError(
                "assessment_page_contract_invalid"
            )
        if page.offset != query.offset or page.limit != query.limit:
            raise QualityAssessmentPortContractError(
                "assessment_page_window_mismatch"
            )
        if any(not isinstance(item, AssessmentReceiptView) for item in page.items):
            raise QualityAssessmentPortContractError(
                "assessment_page_item_invalid"
            )
        expected_order = sorted(
            page.items,
            key=lambda item: (item.receipt.created_at, item.receipt.id),
            reverse=True,
        )
        if list(page.items) != expected_order:
            raise QualityAssessmentPortContractError(
                "assessment_page_order_mismatch"
            )
        for item in page.items:
            receipt = item.receipt
            if (
                receipt.subject.identity != query.subject
                or (
                    query.assessment_kind is not None
                    and receipt.assessment_kind is not query.assessment_kind
                )
            ):
                raise QualityAssessmentPortContractError(
                    "assessment_page_scope_mismatch"
                )
            assert query.current_subject_version is not None
            expected_freshness = evaluate_assessment_currentness(
                receipt,
                current_subject=AssessmentSubjectRef(
                    board_id=query.subject.board_id,
                    subject_type=query.subject.subject_type,
                    subject_id=query.subject.subject_id,
                    subject_version=query.current_subject_version,
                    subject_edition=query.current_subject_edition,
                ),
                current_digests=(
                    receipt.digests
                    if query.current_subject_edition is not None
                    else _current_digests_for_receipt(
                        receipt,
                        inputs=query.currentness_inputs,
                        fallback=query.current_digests,
                    )
                ),
            )
            if query.current_subject_edition is not None:
                expected_state = (
                    AssessmentReceiptState.CURRENT
                    if item.is_head and expected_freshness.current
                    else AssessmentReceiptState.PREVIOUS
                )
            else:
                expected_state = (
                    AssessmentReceiptState.SUPERSEDED
                    if not item.is_head
                    else (
                        AssessmentReceiptState.CURRENT
                        if expected_freshness.current
                        else AssessmentReceiptState.STALE
                    )
                )
            if (
                item.freshness != expected_freshness
                or item.state is not expected_state
                or (query.state is not None and expected_state is not query.state)
            ):
                raise QualityAssessmentPortContractError(
                    "assessment_page_scope_mismatch"
                )
        return page

    async def list_findings(
        self,
        query: FindingListQuery,
        *,
        persistence: QualityAssessmentPersistencePort,
    ) -> QualityPage[QualityFinding]:
        if (
            not isinstance(query, FindingListQuery)
            or not isinstance(query.board_id, str)
            or not query.board_id.strip()
            or not isinstance(query.subject_type, AssessmentSubjectType)
            or not isinstance(query.subject_id, str)
            or not query.subject_id.strip()
            or (
                query.receipt_id is not None
                and (
                    not isinstance(query.receipt_id, str)
                    or not query.receipt_id.strip()
                )
            )
            or (
                query.assessment_kind is not None
                and not isinstance(query.assessment_kind, AssessmentKind)
            )
            or (
                query.category_code is not None
                and (
                    not isinstance(query.category_code, str)
                    or not query.category_code.strip()
                )
            )
            or (
                query.severity is not None
                and not isinstance(query.severity, FindingSeverity)
            )
        ):
            raise QualityAssessmentValidationError(
                "finding_list_query_invalid",
                category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
            )
        _strict_page_window(query.offset, query.limit)
        if query.cursor is not None and (
            not isinstance(query.cursor, QualityPageCursor)
            or query.cursor.offset != query.offset
        ):
            raise QualityAssessmentValidationError(
                "finding_cursor_invalid",
                category=QualityAssessmentErrorCategory.INVALID_ARGUMENT,
            )
        page = await persistence.list_findings(query)
        if not isinstance(page, QualityPage):
            raise QualityAssessmentPortContractError(
                "finding_page_contract_invalid"
            )
        if page.offset != query.offset or page.limit != query.limit:
            raise QualityAssessmentPortContractError(
                "finding_page_window_mismatch"
            )
        if any(not isinstance(item, QualityFinding) for item in page.items):
            raise QualityAssessmentPortContractError(
                "finding_page_item_invalid"
            )
        expected_order = sorted(
            page.items,
            key=lambda item: (item.created_at, item.id),
            reverse=True,
        )
        if list(page.items) != expected_order:
            raise QualityAssessmentPortContractError(
                "finding_page_order_mismatch"
            )
        for item in page.items:
            anchor = item.anchor
            if (
                anchor.board_id != query.board_id
                or anchor.subject_type is not query.subject_type
                or anchor.subject_id != query.subject_id
                or (
                    query.receipt_id is not None
                    and item.receipt_id != query.receipt_id
                )
                or (
                    query.assessment_kind is not None
                    and item.assessment_kind is not query.assessment_kind
                )
                or (
                    query.category_code is not None
                    and item.category_code != query.category_code
                )
                or (
                    query.severity is not None
                    and item.severity is not query.severity
                )
            ):
                raise QualityAssessmentPortContractError(
                    "finding_page_scope_mismatch"
                )
        return page


def _quality_gate_preview(
    receipt: AssessmentReceipt,
    *,
    currentness: AssessmentCurrentness,
    gate_inputs: tuple[QualityGateInput, ...],
) -> QualityGatePreview:
    """Project the one real SK-A gate; all other kinds remain advisory."""

    is_ambiguity_gate = (
        receipt.assessment_kind is AssessmentKind.AMBIGUITY
        and receipt.subject.subject_type
        in {
            AssessmentSubjectType.IDEATION,
            AssessmentSubjectType.REFINEMENT,
        }
    )
    if not is_ambiguity_gate:
        return QualityGatePreview(
            applicable=False,
            enabled=False,
            allowed=True,
            reason_code="not_applicable",
            threshold=None,
            score=receipt.score,
            skipped=False,
        )

    matches = tuple(
        item
        for item in gate_inputs
        if item.assessment_kind is receipt.assessment_kind
    )
    if len(matches) != 1:
        raise QualityAssessmentPortContractError(
            "quality_gate_input_missing"
            if not matches
            else "quality_gate_input_ambiguous"
        )
    gate = matches[0]
    if not gate.applicable or gate.threshold is None:
        raise QualityAssessmentPortContractError(
            "quality_gate_input_invalid"
        )
    if not gate.enabled:
        allowed = True
        reason = AmbiguityGateReason.DISABLED
    elif gate.skipped:
        allowed = True
        reason = AmbiguityGateReason.SKIPPED
    elif not currentness.current:
        allowed = False
        reason = AmbiguityGateReason.ASSESSMENT_STALE
    elif receipt.score > gate.threshold:
        allowed = False
        reason = AmbiguityGateReason.SCORE_EXCEEDS_THRESHOLD
    else:
        allowed = True
        reason = AmbiguityGateReason.READY
    return QualityGatePreview(
        applicable=True,
        enabled=gate.enabled,
        allowed=allowed,
        reason_code=reason.value,
        threshold=gate.threshold,
        score=receipt.score,
        skipped=gate.skipped,
    )


def _current_digests_for_receipt(
    receipt: AssessmentReceipt,
    *,
    inputs: tuple[AssessmentCurrentnessInput, ...],
    fallback: AssessmentDigestSet | None,
) -> AssessmentDigestSet:
    """Select current inputs by the receipt's complete persisted identity."""

    matches = tuple(
        item
        for item in inputs
        if item.assessment_kind is receipt.assessment_kind
        and item.origin is receipt.origin
        and item.source is receipt.source
    )
    if len(matches) == 1:
        return matches[0].digests
    if len(matches) > 1:
        raise QualityAssessmentPortContractError(
            "assessment_currentness_identity_ambiguous"
        )
    if isinstance(fallback, AssessmentDigestSet):
        return fallback
    raise QualityAssessmentPortContractError(
        "assessment_currentness_identity_unsupported"
    )


__all__ = [
    "CurrentAssessmentView",
    "QualityGatePreview",
    "QualityAssessmentConflictError",
    "QualityAssessmentError",
    "QualityAssessmentErrorCategory",
    "QualityAssessmentForbiddenError",
    "QualityAssessmentNotFoundError",
    "QualityAssessmentPortContractError",
    "QualityAssessmentService",
    "QualityAssessmentValidationError",
]
