from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.quality_assessment import (
    AssessmentKind,
    AssessmentOrigin,
    AssessmentOutcome,
    AssessmentReceipt,
    AssessmentSource,
    AssessmentSubjectHead,
    AssessmentSubjectRef,
    AssessmentSubjectType,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.services.ambiguity_assessment import (
    AMBIGUITY_SCALE,
    AmbiguityGateConfiguration,
    AmbiguityGateError,
    AmbiguityGateReason,
    AmbiguityGateService,
    ambiguity_anchor_catalog,
    ambiguity_digest_set,
    ambiguity_versions_v1,
    resolve_ambiguity_gate_configuration,
)

NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


def test_zero_is_a_valid_no_ambiguity_score() -> None:
    assert AMBIGUITY_SCALE.minimum == 0
    assert AMBIGUITY_SCALE.validate_score(0) == 0


def _refinement() -> SimpleNamespace:
    return SimpleNamespace(
        id="ref_1",
        board_id="board_1",
        version=4,
        edition=1,
        title="Bounded checkout",
        description="Decide the payment retry behavior.",
        in_scope=["payment authorization"],
        out_of_scope=["settlement"],
        analysis="Timeouts require an explicit retry budget.",
        decisions=["Retry at most twice."],
    )


def _qa(*, answer: str | None = "Two retries") -> list[dict[str, object]]:
    return [
        {
            "id": "qa_1",
            "revision": 1,
            "question": "What is the retry budget?",
            "question_type": "text",
            "choices": [],
            "allow_free_text": True,
            "answer": answer,
            "selected": [],
            "answered_at": NOW if answer is not None else None,
            "lifecycle": "active",
            "tombstoned": False,
        }
    ]


def _configuration(
    *,
    required: bool = True,
    maximum_score: int = 3,
) -> AmbiguityGateConfiguration:
    return AmbiguityGateConfiguration(
        required=required,
        maximum_score=maximum_score,
    )


def _receipt(
    *,
    score: float = 2,
    subject: SimpleNamespace | None = None,
    qa_items: list[dict[str, object]] | None = None,
    configuration: AmbiguityGateConfiguration | None = None,
) -> AssessmentReceipt:
    resolved_subject = subject or _refinement()
    resolved_configuration = configuration or _configuration()
    digests = ambiguity_digest_set(
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject=resolved_subject,
        qa_items=qa_items if qa_items is not None else _qa(),
        configuration=resolved_configuration,
    )
    subject_ref = AssessmentSubjectRef(
        board_id=resolved_subject.board_id,
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id=resolved_subject.id,
        subject_version=resolved_subject.version,
        subject_edition=resolved_subject.edition,
    )
    return AssessmentReceipt(
        id="qar_1",
        subject=subject_ref,
        assessment_kind=AssessmentKind.AMBIGUITY,
        origin=AssessmentOrigin.HUMAN_OR_AGENT,
        source=AssessmentSource.NATIVE,
        channel="mcp",
        outcome=AssessmentOutcome.RECORDED,
        scale=AMBIGUITY_SCALE,
        score=score,
        justification="The score is supported by the pinpointed findings.",
        digests=digests,
        versions=ambiguity_versions_v1(),
        run_identity_digest=canonical_sha256(
            [subject_ref.subject_version, digests.input_digest],
        ),
        authority_digest=canonical_sha256("authority"),
        idempotency_key="ambiguity-idem-1",
        request_digest=canonical_sha256("request"),
        created_by="agent_1",
        created_at=NOW,
    )


class _Persistence:
    def __init__(self, receipt: AssessmentReceipt | None) -> None:
        self.receipt = receipt
        self.calls = 0

    async def get_current(self, **_: object):
        self.calls += 1
        if self.receipt is None:
            return None
        receipt = self.receipt
        return (
            receipt,
            AssessmentSubjectHead(
                board_id=receipt.subject.board_id,
                subject_type=receipt.subject.subject_type,
                subject_id=receipt.subject.subject_id,
                assessment_kind=receipt.assessment_kind,
                receipt_id=receipt.id,
                revision=1,
                updated_at=NOW,
            ),
        )


def test_refinement_settings_are_legacy_safe_and_strict_on_write_model() -> None:
    assert resolve_ambiguity_gate_configuration("refinement", {}) == (
        AmbiguityGateConfiguration(required=False, maximum_score=3)
    )
    assert resolve_ambiguity_gate_configuration(
        "refinement",
        {
            "require_refinement_ambiguity_gate": True,
            "max_refinement_ambiguity": 5,
        },
    ) == AmbiguityGateConfiguration(required=True, maximum_score=5)
    assert resolve_ambiguity_gate_configuration(
        "refinement",
        {
            "require_refinement_ambiguity_gate": "false",
            "max_refinement_ambiguity": "3",
        },
    ) == AmbiguityGateConfiguration(required=False, maximum_score=3)
    assert resolve_ambiguity_gate_configuration(
        "refinement",
        {"require_refinement_ambiguity_gate": "invalid-legacy-value"},
    ) == AmbiguityGateConfiguration(required=True, maximum_score=3)
    with pytest.raises(ValueError, match="ambiguity_gate_threshold_invalid"):
        AmbiguityGateConfiguration(required=True, maximum_score=6)


def test_anchor_catalog_is_closed_to_fields_children_and_qa_ids() -> None:
    subject = _refinement()
    subject.decisions = [{"id": "decision_1", "text": "Retry twice"}]

    anchors = ambiguity_anchor_catalog(
        subject_type="refinement",
        subject=subject,
        qa_items=_qa(),
    )

    assert "analysis" in anchors.fields
    assert "scope" not in anchors.fields
    assert anchors.structured_child_ids == frozenset({"decision_1"})
    assert anchors.qa_ids == frozenset({"qa_1"})


@pytest.mark.asyncio
async def test_disabled_and_human_skip_do_not_read_assessment_storage() -> None:
    persistence = _Persistence(None)
    service = AmbiguityGateService(persistence)  # type: ignore[arg-type]
    subject = _refinement()

    disabled = await service.evaluate(
        board_id=subject.board_id,
        subject_type="refinement",
        subject=subject,
        board_settings={},
        qa_items=_qa(),
    )
    skipped = await service.evaluate(
        board_id=subject.board_id,
        subject_type="refinement",
        subject=subject,
        board_settings={
            "require_refinement_ambiguity_gate": True,
            "max_refinement_ambiguity": 3,
        },
        qa_items=_qa(),
        skipped=True,
    )

    assert disabled.reason_code is AmbiguityGateReason.DISABLED
    assert skipped.reason_code is AmbiguityGateReason.SKIPPED
    assert persistence.calls == 0


@pytest.mark.asyncio
async def test_gate_accepts_only_current_receipt_at_or_below_threshold() -> None:
    subject = _refinement()
    receipt = _receipt(subject=subject)
    service = AmbiguityGateService(_Persistence(receipt))  # type: ignore[arg-type]

    decision = await service.evaluate(
        board_id=subject.board_id,
        subject_type="refinement",
        subject=subject,
        board_settings={
            "require_refinement_ambiguity_gate": True,
            "max_refinement_ambiguity": 3,
        },
        qa_items=_qa(),
    )

    assert decision.reason_code is AmbiguityGateReason.READY
    assert decision.score == 2
    assert decision.receipt_id == receipt.id
    assert decision.currentness is not None and decision.currentness.current


@pytest.mark.asyncio
async def test_gate_reason_codes_distinguish_missing_and_over_threshold() -> None:
    subject = _refinement()
    settings = {
        "require_refinement_ambiguity_gate": True,
        "max_refinement_ambiguity": 3,
    }
    with pytest.raises(AmbiguityGateError) as missing:
        await AmbiguityGateService(_Persistence(None)).evaluate(  # type: ignore[arg-type]
            board_id=subject.board_id,
            subject_type="refinement",
            subject=subject,
            board_settings=settings,
            qa_items=_qa(),
        )
    assert missing.value.code == AmbiguityGateReason.ASSESSMENT_MISSING.value

    current_receipt = _receipt(subject=subject)
    changed = SimpleNamespace(**{**vars(subject), "analysis": "A different analysis"})
    content_changed = await AmbiguityGateService(  # type: ignore[arg-type]
        _Persistence(current_receipt)
    ).evaluate(
        board_id=subject.board_id,
        subject_type="refinement",
        subject=changed,
        board_settings=settings,
        qa_items=_qa(),
    )
    assert content_changed.reason_code is AmbiguityGateReason.READY
    assert content_changed.currentness is not None
    assert content_changed.currentness.current is True

    high_receipt = _receipt(score=4, subject=subject)
    with pytest.raises(AmbiguityGateError) as high:
        await AmbiguityGateService(  # type: ignore[arg-type]
            _Persistence(high_receipt)
        ).evaluate(
            board_id=subject.board_id,
            subject_type="refinement",
            subject=subject,
            board_settings=settings,
            qa_items=_qa(),
        )
    assert high.value.code == AmbiguityGateReason.SCORE_EXCEEDS_THRESHOLD.value


@pytest.mark.asyncio
async def test_answer_changes_stay_current_and_gate_settings_apply_at_gate_time() -> None:
    subject = _refinement()
    receipt = _receipt(subject=subject)
    service = AmbiguityGateService(_Persistence(receipt))  # type: ignore[arg-type]

    clarified = await service.evaluate(
        board_id=subject.board_id,
        subject_type="refinement",
        subject=subject,
        board_settings={
            "require_refinement_ambiguity_gate": True,
            "max_refinement_ambiguity": 3,
        },
        qa_items=_qa(answer="Three retries"),
    )
    assert clarified.reason_code is AmbiguityGateReason.READY
    assert clarified.currentness is not None and clarified.currentness.current

    with pytest.raises(AmbiguityGateError) as stricter_threshold:
        await service.evaluate(
            board_id=subject.board_id,
            subject_type="refinement",
            subject=subject,
            board_settings={
                "require_refinement_ambiguity_gate": True,
                "max_refinement_ambiguity": 1,
            },
            qa_items=_qa(),
        )
    assert (
        stricter_threshold.value.code
        == AmbiguityGateReason.SCORE_EXCEEDS_THRESHOLD.value
    )
    assert "stale_reasons" not in stricter_threshold.value.details

    relaxed = await service.evaluate(
        board_id=subject.board_id,
        subject_type="refinement",
        subject=subject,
        board_settings={
            "require_refinement_ambiguity_gate": True,
            "max_refinement_ambiguity": 5,
        },
        qa_items=_qa(),
    )
    assert relaxed.allowed is True
    assert relaxed.reason_code is AmbiguityGateReason.READY
    assert relaxed.currentness is not None
    assert relaxed.currentness.current is True

    baseline = ambiguity_digest_set(
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject=subject,
        qa_items=_qa(),
        configuration=_configuration(required=True, maximum_score=3),
    )
    disabled = ambiguity_digest_set(
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject=subject,
        qa_items=_qa(),
        configuration=_configuration(required=False, maximum_score=3),
    )
    changed_threshold = ambiguity_digest_set(
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject=subject,
        qa_items=_qa(),
        configuration=_configuration(required=True, maximum_score=5),
    )
    assert baseline.policy_digest == disabled.policy_digest
    assert baseline.policy_digest == changed_threshold.policy_digest
