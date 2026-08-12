from __future__ import annotations

from datetime import datetime, timezone

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
)
from okto_pulse.core.application.use_cases.submit_spec_validation import (
    SubmitSpecValidationCommand,
)
from okto_pulse.core.application.use_cases.validation_cycle import (
    GetValidationCycleCommand,
    GetValidationCyclesCommand,
    GetValidationTechnicalAuditCommand,
    ValidationCycleReadUseCases,
)
from okto_pulse.core.domain.quality_assessment import AssessmentSubjectType
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.domain.validation_cycle import (
    RequirementLintAnchor,
    RequirementLintPreflight,
    ValidationCycleCheckSummary,
    ValidationCycleContractError,
    ValidationCycleResultSummary,
    ValidationCycleResultType,
    ValidationCycleState,
    ValidationCycleSummary,
    ValidationCycleSubjectRef,
    ValidationEditionExceptionAudit,
    ValidationEditionExceptionType,
    ValidationSubmissionFence,
    ValidationTechnicalAudit,
    ValidationTechnicalAuditDetails,
)
from okto_pulse.core.inbound.human_validation_cycle_error import (
    project_subject_edit_requires_draft_error,
)
from okto_pulse.core.models.validation_cycle import (
    project_requirement_lint_preflight,
    project_validation_cycle,
    project_validation_technical_audit,
)
from okto_pulse.core.models.schemas import SpecValidationSubmit
from okto_pulse.core.domain.human_validation_cycle import (
    SubjectEditRequiresDraftError,
)


_DIGEST = "a" * 64


def test_spec_validation_submit_rejects_mixed_formal_and_legacy_shapes() -> None:
    with pytest.raises(
        ValueError,
        match="formal and legacy validation shapes are mutually exclusive",
    ):
        SpecValidationSubmit.model_validate(
            {
                "expected_validation_edition": 3,
                "expected_spec_version": 17,
                "expected_head_revision": 0,
                "score": 92,
                "summary": "The current validation edition is ready.",
                "completeness": 92,
            }
        )

    # MCP builds a plain mapping and therefore relies on the shared command,
    # rather than Pydantic, to enforce the exact same one-of contract.
    with pytest.raises(
        CommandValidationError,
        match="formal and legacy validation shapes are mutually exclusive",
    ):
        SubmitSpecValidationCommand(
            "spec-1",
            {
                "expected_validation_edition": 3,
                "expected_spec_version": 17,
                "expected_head_revision": 0,
                "score": 92,
                "summary": "The current validation edition is ready.",
                "completeness": 92,
            },
        ).validate()


def _fence(*, edition: int = 3) -> ValidationSubmissionFence:
    return ValidationSubmissionFence(
        expected_validation_edition=edition,
        expected_subject_version=17,
        expected_head_revision=0,
    )


def _result(
    *,
    edition: int | None = 3,
    result_id: str = "validation-current",
) -> ValidationCycleResultSummary:
    return ValidationCycleResultSummary(
        result_id=result_id,
        result_type=ValidationCycleResultType.SPEC_VALIDATION,
        subject_edition=edition,
        status="approved",
        summary={"outcome": "approved", "counts": {"passed": 8}},
    )


def _checks() -> tuple[ValidationCycleCheckSummary, ...]:
    return tuple(
        ValidationCycleCheckSummary(
            result_type=result_type,
            status="pending",
            summary="Not started",
        )
        for result_type in (
            ValidationCycleResultType.REQUIREMENT_LINT,
            ValidationCycleResultType.CURATED_CHECKLIST,
            ValidationCycleResultType.POLICY_COMPLIANCE,
        )
    )


def _spec_sections() -> tuple[ValidationCycleResultType, ...]:
    return (
        ValidationCycleResultType.SPEC_VALIDATION,
        ValidationCycleResultType.REQUIREMENT_LINT,
        ValidationCycleResultType.CURATED_CHECKLIST,
        ValidationCycleResultType.POLICY_COMPLIANCE,
    )


def _exception_types() -> tuple[ValidationEditionExceptionType, ...]:
    return tuple(ValidationEditionExceptionType)


def test_validation_cycle_projection_separates_current_and_previous_results() -> None:
    cycle = ValidationCycleSummary(
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="spec-1",
        edition=3,
        status="validation",
        cycle_state=ValidationCycleState.IN_PROGRESS,
        current_result=_result(),
        previous_result_count=2,
        previous_results=(_result(edition=2, result_id="validation-previous"),),
        checks=_checks(),
        remaining_actions=("complete_requirement_lint",),
        submission_fence=_fence(),
        visible_sections=_spec_sections(),
    )

    payload = project_validation_cycle(cycle)

    assert payload["subject_id"] == "spec-1"
    assert payload["subject_status"] == "validation"
    assert payload["visible_sections"] == [
        "spec_validation",
        "requirement_lint",
        "curated_checklist",
        "policy_compliance",
    ]
    assert "id" not in payload
    assert "status" not in payload
    assert payload["current_result"]["result_id"] == "validation-current"
    assert payload["previous_result_count"] == 2
    assert [item["result_id"] for item in payload["previous_results"]] == [
        "validation-previous"
    ]
    assert [item["result_type"] for item in payload["checks"]] == [
        "requirement_lint",
        "curated_checklist",
        "policy_compliance",
    ]
    assert payload["submission_fence"] == {
        "expected_validation_edition": 3,
        "expected_subject_version": 17,
        "expected_head_revision": 0,
    }


def test_partial_spec_projection_omits_the_hidden_validation_section() -> None:
    lint = ValidationCycleCheckSummary(
        result_type=ValidationCycleResultType.REQUIREMENT_LINT,
        status="needs_attention",
        summary="2 findings",
    )
    cycle = ValidationCycleSummary(
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="spec-1",
        edition=3,
        status="approved",
        cycle_state=None,
        current_result=None,
        previous_result_count=None,
        submission_fence=None,
        checks=(lint,),
        remaining_actions=(),
        visible_sections=(ValidationCycleResultType.REQUIREMENT_LINT,),
    )

    payload = project_validation_cycle(cycle)

    assert payload["visible_sections"] == ["requirement_lint"]
    assert payload["checks"] == [
        {
            "result_type": "requirement_lint",
            "status": "needs_attention",
            "summary": "2 findings",
        }
    ]
    assert payload["remaining_actions"] == []
    for hidden_key in (
        "cycle_state",
        "current_result",
        "previous_result_count",
        "previous_results",
        "submission_fence",
    ):
        assert hidden_key not in payload


def test_partial_spec_contract_rejects_cross_section_data() -> None:
    with pytest.raises(
        ValidationCycleContractError,
        match="validation_cycle_hidden_state_present",
    ):
        ValidationCycleSummary(
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="spec-1",
            edition=3,
            status="approved",
            cycle_state=ValidationCycleState.COMPLETED,
            current_result=None,
            previous_result_count=None,
            submission_fence=None,
            checks=(
                ValidationCycleCheckSummary(
                    ValidationCycleResultType.REQUIREMENT_LINT,
                    "passed",
                    "No findings",
                ),
            ),
            visible_sections=(ValidationCycleResultType.REQUIREMENT_LINT,),
        )


def test_visibility_contracts_are_required_and_summary_cannot_be_empty() -> None:
    with pytest.raises(TypeError, match="visible_sections"):
        ValidationCycleSummary(  # type: ignore[call-arg]
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="spec-1",
            edition=3,
            status="draft",
            cycle_state=ValidationCycleState.NOT_STARTED,
            current_result=None,
            previous_result_count=0,
            submission_fence=_fence(),
        )
    with pytest.raises(
        ValidationCycleContractError,
        match="validation_cycle_visible_sections_invalid",
    ):
        ValidationCycleSummary(
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="spec-1",
            edition=3,
            status="draft",
            cycle_state=None,
            current_result=None,
            previous_result_count=None,
            submission_fence=None,
            visible_sections=(),
        )
    with pytest.raises(TypeError, match="visible_exception_types"):
        ValidationTechnicalAuditDetails(  # type: ignore[call-arg]
            receipt_id="receipt-1",
            subject_version=1,
            head_revision=0,
            digests={},
        )


def test_legacy_null_edition_is_previous_and_never_current() -> None:
    legacy = _result(edition=None, result_id="validation-legacy")
    cycle = ValidationCycleSummary(
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="spec-1",
        edition=3,
        status="draft",
        cycle_state=ValidationCycleState.NOT_STARTED,
        current_result=None,
        previous_result_count=1,
        previous_results=(legacy,),
        checks=_checks(),
        submission_fence=_fence(),
        visible_sections=_spec_sections(),
    )

    payload = project_validation_cycle(cycle)
    assert payload["current_result"] is None
    assert payload["previous_results"] == [
        {
            "result_id": "validation-legacy",
            "result_type": "spec_validation",
            "subject_edition": None,
            "status": "approved",
            "summary": {"outcome": "approved", "counts": {"passed": 8}},
        }
    ]

    with pytest.raises(
        ValidationCycleContractError,
        match="validation_cycle_current_result_scope_mismatch",
    ):
        ValidationCycleSummary(
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="spec-1",
            edition=3,
            status="validation",
            cycle_state=ValidationCycleState.COMPLETED,
            current_result=legacy,
            previous_result_count=0,
            checks=_checks(),
            submission_fence=_fence(),
            visible_sections=_spec_sections(),
        )


@pytest.mark.parametrize(
    "summary",
    [
        {"receipt_id": "hidden"},
        {"nested": {"head_revision": 1}},
        {"nested": [{"subject_version": 2}]},
        {"nested": {"deeper": {"digests": {"ruleset": _DIGEST}}}},
        {"nested": {"stale_reasons": ["content_changed"]}},
    ],
)
def test_human_summary_recursively_rejects_technical_audit_keys(
    summary: object,
) -> None:
    with pytest.raises(
        ValidationCycleContractError,
        match="validation_cycle_summary_contains_technical_audit",
    ):
        ValidationCycleResultSummary(
            result_id="result-1",
            result_type=ValidationCycleResultType.SPEC_VALIDATION,
            subject_edition=1,
            status="approved",
            summary=summary,  # type: ignore[arg-type]
        )


def test_requirement_lint_preflight_uses_subject_edition_fence() -> None:
    preflight = RequirementLintPreflight(
        subject_edition=3,
        subject_status="approved",
        ruleset_digest=_DIGEST,
        requirement_anchors=(
            RequirementLintAnchor(
                anchor_type="functional_requirement",
                anchor_ref="fr-1",
                excerpt_hash="b" * 64,
            ),
        ),
        submission_fence=_fence(),
    )

    payload = project_requirement_lint_preflight(preflight)

    assert payload["assessment_kind"] == "requirement_lint"
    assert payload["subject_status"] == "approved"
    assert payload["requirement_anchors"][0]["anchor_ref"] == "fr-1"
    assert payload["submission_fence"]["expected_subject_edition"] == 3
    assert "expected_validation_edition" not in payload["submission_fence"]


def test_technical_audit_is_lazy_shape_and_exception_is_edition_bound() -> None:
    audit = ValidationTechnicalAudit(
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="spec-1",
        result_id="validation-previous",
        result_type=ValidationCycleResultType.SPEC_VALIDATION,
        subject_edition=2,
        technical_audit=ValidationTechnicalAuditDetails(
            receipt_id="receipt-1",
            subject_version=12,
            head_revision=1,
            digests={"ruleset_digest": _DIGEST},
            visible_exception_types=_exception_types(),
            exceptions=(
                ValidationEditionExceptionAudit(
                    exception_id="skip-1",
                    exception_type=ValidationEditionExceptionType.POLICY_WAIVER,
                    subject_edition=2,
                    status="accepted",
                    reason="Approved exception",
                    actor_id="reviewer-1",
                    recorded_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
                ),
            ),
        ),
    )

    payload = project_validation_technical_audit(audit)

    assert set(payload) == {
        "subject_type",
        "subject_id",
        "result_id",
        "result_type",
        "subject_edition",
        "technical_audit",
    }
    assert payload["technical_audit"]["exceptions"][0]["exception_id"] == "skip-1"
    assert payload["technical_audit"]["visible_exception_types"] == [
        "ambiguity_gate_skip",
        "policy_skip",
        "policy_waiver",
    ]


def test_technical_audit_rejects_an_exception_outside_caller_visibility() -> None:
    with pytest.raises(
        ValidationCycleContractError,
        match="validation_audit_hidden_exception_present",
    ):
        ValidationTechnicalAuditDetails(
            receipt_id="receipt-1",
            subject_version=12,
            head_revision=1,
            digests={},
            visible_exception_types=(),
            exceptions=(
                ValidationEditionExceptionAudit(
                    exception_id="waiver-1",
                    exception_type=ValidationEditionExceptionType.POLICY_WAIVER,
                    subject_edition=2,
                    status="approved",
                    reason="Restricted waiver",
                    actor_id="reviewer-1",
                    recorded_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
                ),
            ),
        )


def test_technical_audit_rejects_exception_from_another_edition() -> None:
    with pytest.raises(
        ValidationCycleContractError,
        match="validation_audit_exception_edition_mismatch",
    ):
        ValidationTechnicalAudit(
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="spec-1",
            result_id="result-1",
            result_type=ValidationCycleResultType.SPEC_VALIDATION,
            subject_edition=2,
            technical_audit=ValidationTechnicalAuditDetails(
                receipt_id="receipt-1",
                subject_version=12,
                head_revision=1,
                digests={},
                visible_exception_types=_exception_types(),
                exceptions=(
                    ValidationEditionExceptionAudit(
                        exception_id="skip-1",
                        exception_type=ValidationEditionExceptionType.POLICY_SKIP,
                        subject_edition=1,
                        status="accepted",
                        reason="Historic skip",
                        actor_id="reviewer-1",
                        recorded_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
                    ),
                ),
            ),
        )


def test_legacy_null_edition_technical_audit_remains_readable() -> None:
    audit = ValidationTechnicalAudit(
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="spec-1",
        result_id="validation-legacy",
        result_type=ValidationCycleResultType.SPEC_VALIDATION,
        subject_edition=None,
        technical_audit=ValidationTechnicalAuditDetails(
            receipt_id="receipt-legacy",
            subject_version=1,
            head_revision=1,
            digests={},
            visible_exception_types=_exception_types(),
        ),
    )

    payload = project_validation_technical_audit(audit)
    assert payload["subject_edition"] is None
    assert payload["technical_audit"]["receipt_id"] == "receipt-legacy"


@pytest.mark.asyncio
async def test_cycle_use_case_fails_closed_when_reader_eager_loads_history() -> None:
    class _Reader:
        async def get_validation_cycle(self, **_: object) -> ValidationCycleSummary:
            return ValidationCycleSummary(
                subject_type=AssessmentSubjectType.SPEC,
                subject_id="spec-1",
                edition=3,
                status="validation",
                cycle_state=ValidationCycleState.IN_PROGRESS,
                current_result=_result(),
                previous_result_count=1,
                previous_results=(_result(edition=2, result_id="previous"),),
                checks=_checks(),
                submission_fence=_fence(),
                visible_sections=_spec_sections(),
            )

        async def get_result_technical_audit(self, **_: object) -> object:
            raise AssertionError("not called")

    actor = ActorContext(
        "reviewer-1",
        "rest",
        realm_scope=RealmScope.tenant("realm-1"),
    )

    with pytest.raises(RuntimeError, match="validation_cycle_reader_scope_mismatch"):
        await ValidationCycleReadUseCases(reader=_Reader()).get_cycle(
            GetValidationCycleCommand(
                subject_type=AssessmentSubjectType.SPEC,
                subject_id="spec-1",
                include_previous=False,
            ),
            actor=actor,
        )


@pytest.mark.asyncio
async def test_batch_use_case_rejects_non_summary_before_scope_projection() -> None:
    class _Reader:
        async def get_validation_cycles(self, **_: object) -> tuple[object, ...]:
            return (object(),)

    actor = ActorContext(
        "reviewer-1",
        "rest",
        realm_scope=RealmScope.tenant("realm-1"),
    )
    with pytest.raises(
        RuntimeError,
        match="validation_cycle_batch_reader_result_invalid",
    ):
        await ValidationCycleReadUseCases(reader=_Reader()).get_many(
            GetValidationCyclesCommand(
                (
                    ValidationCycleSubjectRef(
                        AssessmentSubjectType.SPEC,
                        "spec-1",
                    ),
                )
            ),
            actor=actor,
        )


@pytest.mark.asyncio
async def test_audit_use_case_rejects_result_id_collision_across_subject_scope() -> (
    None
):
    class _Reader:
        async def get_result_technical_audit(
            self, **_: object
        ) -> ValidationTechnicalAudit:
            return ValidationTechnicalAudit(
                subject_type=AssessmentSubjectType.SPEC,
                subject_id="spec-other",
                result_id="shared-result-id",
                result_type=ValidationCycleResultType.REQUIREMENT_LINT,
                subject_edition=1,
                technical_audit=ValidationTechnicalAuditDetails(
                    receipt_id="shared-result-id",
                    subject_version=1,
                    head_revision=1,
                    digests={},
                    visible_exception_types=(),
                ),
            )

    actor = ActorContext(
        "reviewer-1",
        "rest",
        realm_scope=RealmScope.tenant("realm-1"),
    )
    with pytest.raises(RuntimeError, match="validation_audit_reader_scope_mismatch"):
        await ValidationCycleReadUseCases(reader=_Reader()).get_technical_audit(
            GetValidationTechnicalAuditCommand(
                subject_type=AssessmentSubjectType.SPEC,
                subject_id="spec-1",
                result_id="shared-result-id",
                result_type=ValidationCycleResultType.SPEC_VALIDATION,
            ),
            actor=actor,
        )


def test_draft_only_error_has_stable_transport_neutral_conflict_code() -> None:
    payload = project_subject_edit_requires_draft_error(
        SubjectEditRequiresDraftError("spec", "spec-1", "validation")
    )

    assert payload["code"] == "subject_edit_requires_draft"
    assert payload["category"] == "conflict"
    assert payload["retryable"] is False
    assert payload["details"]["required_status"] == "draft"
