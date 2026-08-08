"""SK-B3 I1 tests for semantic metric admission and immutable receipts."""

from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

import okto_pulse.core.domain.guideline_semantic_assessment as semantic_assessment_domain
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_DOMAIN_CONTRACT_VERSION,
    RESERVED_CONFIDENCE_FIELD,
    BoardGuidelineBinding,
    GuidelineEnforcement,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelinePolicyContractError,
    GuidelineRevision,
    PolicyEntityType,
    PolicySubjectRef,
    PolicySubjectSnapshot,
    guideline_binding_configuration_digest_v1,
    guideline_revision_digest_v2,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    LEGACY_UNKNOWN_SEMANTIC_EDITOR_ID,
    SEMANTIC_GUIDELINE_ASSESSMENT_CONTRACT_VERSION,
    SemanticAssessmentAssessor,
    SemanticAssessmentContractError,
    SemanticAssessmentInadmissibilityCause,
    SemanticAssessmentInadmissibleError,
    SemanticAssessmentState,
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentSubmission,
    SemanticMetricAssessment,
    SemanticMetricOutcome,
    SemanticThresholdSource,
    record_semantic_guideline_assessment,
    semantic_assessment_input_digest_v1,
    semantic_assessment_request_digest_v1,
    semantic_assessment_receipt_digest_v1,
    semantic_binding_head_digest_v1,
    semantic_policy_set_digest_v1,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    QualityAssessmentContractError,
    UnboundFindingAnchor,
)
from okto_pulse.core.ports.guideline_policy import (
    SemanticGuidelineAssessmentPersistencePort,
)


NOW = datetime(2026, 7, 30, 18, tzinfo=timezone.utc)
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DOMAIN_PATHS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "domain"
    / "guideline_policy.py",
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "domain"
    / "guideline_semantic_assessment.py",
)


def _metric(
    metric_id: str,
    code: str,
    *,
    direction: GuidelineMetricDirection = GuidelineMetricDirection.MINIMUM,
    threshold: int = 70,
    targets: tuple[PolicyEntityType, ...] = (PolicyEntityType.SPEC,),
) -> GuidelineMetric:
    return GuidelineMetric(
        metric_id=metric_id,
        code=code,
        title=f"{code} score",
        description=f"Measure {code} semantically.",
        evaluation_rubric=f"Inspect the subject and score {code} from 0 to 100.",
        target_entity_types=targets,
        direction=direction,
        default_threshold=threshold,
    )


def _revision(
    metrics: tuple[GuidelineMetric, ...] | list[GuidelineMetric],
) -> GuidelineRevision:
    return GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Hexagonal architecture",
        content="Keep business rules independent from technical capabilities.",
        metrics=metrics,
        created_by="author-1",
        created_at=NOW,
    )


def _binding(
    revision: GuidelineRevision,
    **overrides: object,
) -> BoardGuidelineBinding:
    values: dict[str, object] = {
        "binding_id": "binding-1",
        "board_id": "board-1",
        "guideline_id": revision.guideline_id,
        "revision_id": revision.revision_id,
        "semantic_version": revision.semantic_version,
        "revision_digest": revision.revision_digest,
        "priority": 0,
        "binding_revision": 3,
        "adopted_by": "owner-1",
        "adopted_at": NOW,
        "enforcement": GuidelineEnforcement.BLOCKING,
        "minimum_confidence": 80,
        "metric_threshold_overrides": {
            "architecture.segregation": 75,
        },
    }
    values.update(overrides)
    return BoardGuidelineBinding(**values)


def _snapshot(
    *,
    target: PolicyEntityType = PolicyEntityType.SPEC,
    version: int = 4,
    last_semantic_editor_id: str = "editor-1",
) -> PolicySubjectSnapshot:
    return PolicySubjectSnapshot(
        subject=PolicySubjectRef(
            board_id="board-1",
            entity_type=target,
            subject_id=f"{target.value}-1",
            subject_version=version,
        ),
        content_digest=DIGEST_A,
        last_semantic_editor_id=last_semantic_editor_id,
        captured_at=NOW,
    )


def _evidence(source_id: str = "spec-1") -> EvidenceRef:
    return EvidenceRef(
        source_type="subject",
        source_id=source_id,
        source_version=4,
        content_hash=DIGEST_A,
    )


def _pinpoint(
    anchor_ref: str = "technical_requirements.tr_hexagonal",
) -> UnboundFindingAnchor:
    return UnboundFindingAnchor(
        anchor_type=FindingAnchorType.STRUCTURED_CHILD,
        anchor_ref=anchor_ref,
        excerpt_hash=DIGEST_B,
    )


def _metric_assessment(
    metric: GuidelineMetric,
    score: int,
) -> SemanticMetricAssessment:
    return SemanticMetricAssessment(
        metric_id=metric.metric_id,
        score=score,
        rationale=f"{metric.code} is supported by the referenced evidence.",
        evidence_refs=(_evidence(),),
        pinpoints=(_pinpoint(),),
    )


def _semantic_fixture(
    *,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
    minimum_confidence: int = 80,
    last_semantic_editor_id: str = "editor-1",
):
    segregation = _metric(
        "metric-segregation",
        "architecture.segregation",
    )
    coupling = _metric(
        "metric-coupling",
        "architecture.coupling",
        direction=GuidelineMetricDirection.MAXIMUM,
        threshold=30,
    )
    refinement_only = _metric(
        "metric-research",
        "architecture.research",
        targets=(PolicyEntityType.REFINEMENT,),
    )
    revision = _revision((segregation, coupling, refinement_only))
    binding = _binding(
        revision,
        enforcement=enforcement,
        minimum_confidence=minimum_confidence,
    )
    snapshot = _snapshot(
        last_semantic_editor_id=last_semantic_editor_id,
    )
    context = SemanticGuidelineAssessmentContext(
        subject_snapshot=snapshot,
        binding=binding,
        revision=revision,
        policy_set_digest=DIGEST_B,
        binding_head_digest="c" * 64,
    )
    return segregation, coupling, refinement_only, revision, binding, context


def _submission(
    context: SemanticGuidelineAssessmentContext,
    metric_results: tuple[SemanticMetricAssessment, ...],
    *,
    confidence: int = 80,
    assessor_agent_id: str = "assessor-1",
    model_id: str | None = "model-1",
) -> SemanticGuidelineAssessmentSubmission:
    return SemanticGuidelineAssessmentSubmission(
        subject=context.subject_snapshot.subject,
        binding_id=context.binding.binding_id,
        expected_binding_revision=context.binding.binding_revision,
        guideline_revision_id=context.revision.revision_id,
        idempotency_key="semantic-assessment:spec-1:v4",
        confidence=confidence,
        assessor=SemanticAssessmentAssessor(
            agent_id=assessor_agent_id,
            model_id=model_id,
        ),
        metric_results=metric_results,
    )


def test_active_contract_exposes_metrics_without_policy_v1_fields() -> None:
    assert GUIDELINE_DOMAIN_CONTRACT_VERSION == "guideline-domain/v2"
    assert (
        SEMANTIC_GUIDELINE_ASSESSMENT_CONTRACT_VERSION
        == "semantic-guideline-assessment/v1"
    )
    assert RESERVED_CONFIDENCE_FIELD == "confidence"
    assert {item.value for item in GuidelineMetricDirection} == {
        "minimum",
        "maximum",
    }
    assert {item.value for item in SemanticAssessmentState} == {
        "passed",
        "metric_threshold_failed",
    }
    assert {field.name for field in fields(GuidelineRevision)} == {
        "revision_id",
        "guideline_id",
        "revision_number",
        "semantic_version",
        "title",
        "content",
        "metrics",
        "created_by",
        "created_at",
        "revision_digest",
        "parent_revision_id",
        "tags",
    }
    assert "rules" not in {field.name for field in fields(GuidelineRevision)}
    assert "default_enforcement" not in {
        field.name for field in fields(BoardGuidelineBinding)
    }
    assert "attributes" not in {
        field.name for field in fields(PolicySubjectSnapshot)
    }


def test_domain_vertical_has_no_transport_persistence_or_model_provider_import() -> None:
    forbidden = {
        "anthropic",
        "fastapi",
        "ladybugdb",
        "openai",
        "react",
        "sqlalchemy",
    }
    for path in DOMAIN_PATHS:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                roots.add(node.module.split(".", 1)[0])
        assert roots.isdisjoint(forbidden)


def test_semantic_assessment_persistence_seam_is_explicit_and_bounded() -> None:
    assert {
        name
        for name in dir(SemanticGuidelineAssessmentPersistencePort)
        if not name.startswith("_")
    } >= {
        "resolve_policy_subject_snapshot",
        "get_semantic_assessment_result_by_idempotency",
        "save_semantic_assessment_result",
        "get_semantic_assessment_receipt",
        "get_current_semantic_assessment_receipt",
    }


@pytest.mark.parametrize(
    ("override", "code"),
    (
        ({"metric_id": "confidence"}, "guideline_metric_confidence_reserved"),
        ({"code": "confidence"}, "guideline_metric_confidence_reserved"),
        ({"code": "Confidence"}, "guideline_metric_confidence_reserved"),
        ({"title": "Confidence"}, "guideline_metric_confidence_reserved"),
        ({"evaluation_rubric": " "}, "guideline_metric_evaluation_rubric_required"),
        ({"target_entity_types": ()}, "guideline_metric_target_entity_types_required"),
        ({"direction": "minimum"}, "guideline_metric_direction_invalid"),
        ({"default_threshold": True}, "guideline_metric_default_threshold_invalid"),
        ({"default_threshold": -1}, "guideline_metric_default_threshold_invalid"),
        ({"default_threshold": 101}, "guideline_metric_default_threshold_invalid"),
        ({"default_threshold": 70.5}, "guideline_metric_default_threshold_invalid"),
    ),
)
def test_metric_rejects_reserved_or_malformed_values(
    override: dict[str, object],
    code: str,
) -> None:
    values: dict[str, object] = {
        "metric_id": "metric-1",
        "code": "architecture.segregation",
        "title": "Segregation",
        "description": "Measure boundary segregation.",
        "evaluation_rubric": "Inspect dependency direction and score it.",
        "target_entity_types": (PolicyEntityType.SPEC,),
        "direction": GuidelineMetricDirection.MINIMUM,
        "default_threshold": 70,
    }
    values.update(override)
    with pytest.raises(GuidelinePolicyContractError, match=code):
        GuidelineMetric(**values)


def test_revision_preserves_metric_order_and_seals_normative_digest() -> None:
    second = _metric("metric-z", "z.metric")
    first = _metric("metric-a", "a.metric")
    mutable_metrics = [second, first]
    revision = _revision(mutable_metrics)
    mutable_metrics.clear()

    assert revision.metrics == (second, first)
    assert revision.revision_digest == guideline_revision_digest_v2(
        semantic_version=revision.semantic_version,
        title=revision.title,
        content=revision.content,
        metrics=(second, first),
        tags=(),
    )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_digest_mismatch",
    ):
        replace(revision, revision_digest=DIGEST_A)
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_duplicate_metric_code",
    ):
        _revision(
            (
                _metric("metric-1", "Architecture.Score"),
                _metric("metric-2", "architecture.score"),
            )
        )


def test_context_only_revision_is_valid_but_cannot_create_fake_evidence() -> None:
    revision = _revision(())
    binding = _binding(
        revision,
        metric_threshold_overrides={},
    )
    context = SemanticGuidelineAssessmentContext(
        subject_snapshot=_snapshot(),
        binding=binding,
        revision=revision,
        policy_set_digest=DIGEST_A,
        binding_head_digest=DIGEST_B,
    )
    assert revision.context_only is True
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_assessment_metric_results_invalid",
    ):
        _submission(context, ())


def test_binding_deep_freezes_code_keyed_overrides_and_seals_configuration() -> None:
    metric = _metric("metric-1", "architecture.segregation")
    revision = _revision((metric,))
    mutable_overrides = {"architecture.segregation": 81}
    binding = _binding(
        revision,
        metric_threshold_overrides=mutable_overrides,
    )
    mutable_overrides["architecture.segregation"] = 1

    assert dict(binding.metric_threshold_overrides) == {
        "architecture.segregation": 81
    }
    assert binding.configuration_digest == (
        guideline_binding_configuration_digest_v1(
            binding_id=binding.binding_id,
            board_id=binding.board_id,
            guideline_id=binding.guideline_id,
            revision_id=binding.revision_id,
            revision_digest=binding.revision_digest,
            priority=binding.priority,
            enforcement=binding.enforcement,
            minimum_confidence=binding.minimum_confidence,
            metric_threshold_overrides=binding.metric_threshold_overrides,
        )
    )
    with pytest.raises(TypeError):
        binding.metric_threshold_overrides["architecture.segregation"] = 2


def test_context_rejects_override_keyed_by_metric_id_instead_of_code() -> None:
    metric = _metric("metric-1", "architecture.segregation")
    revision = _revision((metric,))
    binding = _binding(
        revision,
        metric_threshold_overrides={metric.metric_id: 90},
    )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_assessment_threshold_override_unknown",
    ):
        SemanticGuidelineAssessmentContext(
            subject_snapshot=_snapshot(),
            binding=binding,
            revision=revision,
            policy_set_digest=DIGEST_A,
            binding_head_digest=DIGEST_B,
        )


def test_complete_assessment_is_conjunctive_and_seals_all_evidence() -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    # Caller order is irrelevant; receipt order follows the normative revision.
    submission = _submission(
        context,
        (
            _metric_assessment(coupling, 30),
            _metric_assessment(segregation, 75),
        ),
    )
    result = record_semantic_guideline_assessment(
        submission,
        context,
        receipt_id="receipt-1",
        recorded_at=NOW,
    )
    receipt = result.receipt

    assert receipt.state is SemanticAssessmentState.PASSED
    assert receipt.confidence_admissible is True
    assert receipt.assessor_independent is True
    assert receipt.metric_count == 2
    assert receipt.failed_metric_count == 0
    assert [item.metric_id for item in receipt.metric_results] == [
        segregation.metric_id,
        coupling.metric_id,
    ]
    assert receipt.metric_results[0].threshold_source is (
        SemanticThresholdSource.OVERRIDE
    )
    assert receipt.metric_results[0].effective_threshold == 75
    assert receipt.metric_results[1].threshold_source is (
        SemanticThresholdSource.DEFAULT
    )
    assert receipt.metric_results[1].effective_threshold == 30
    assert {
        item.outcome for item in receipt.metric_results
    } == {SemanticMetricOutcome.PASS}
    assert all(item.rationale for item in receipt.metric_results)
    assert all(item.evidence_refs for item in receipt.metric_results)
    assert all(item.pinpoints for item in receipt.metric_results)
    assert all(
        pinpoint.subject == receipt.subject
        and pinpoint.input_digest == receipt.input_digest
        for item in receipt.metric_results
        for pinpoint in item.pinpoints
    )
    assert receipt.receipt_digest == semantic_assessment_receipt_digest_v1(
        receipt
    )
    with pytest.raises(FrozenInstanceError):
        receipt.state = SemanticAssessmentState.METRIC_THRESHOLD_FAILED


def test_metric_threshold_failure_still_seals_complete_evidence() -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    threshold_failure = record_semantic_guideline_assessment(
        _submission(
            context,
            (
                _metric_assessment(segregation, 74),
                _metric_assessment(coupling, 30),
            ),
            confidence=90,
        ),
        context,
        receipt_id="receipt-failed",
        recorded_at=NOW,
    ).receipt

    assert threshold_failure.state is (
        SemanticAssessmentState.METRIC_THRESHOLD_FAILED
    )
    assert threshold_failure.confidence_admissible is True
    assert threshold_failure.failed_metric_count == 1


def _spy_evidence_construction(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    constructed: list[str] = []
    metric_result_type = semantic_assessment_domain.SemanticMetricResult
    receipt_type = (
        semantic_assessment_domain.SemanticGuidelineAssessmentReceipt
    )

    def metric_result_spy(**kwargs):
        constructed.append("metric_result")
        return metric_result_type(**kwargs)

    def receipt_spy(**kwargs):
        constructed.append("receipt")
        return receipt_type(**kwargs)

    monkeypatch.setattr(
        semantic_assessment_domain,
        "SemanticMetricResult",
        metric_result_spy,
    )
    monkeypatch.setattr(
        semantic_assessment_domain,
        "SemanticGuidelineAssessmentReceipt",
        receipt_spy,
    )
    return constructed


def test_low_confidence_is_rejected_before_any_result_or_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    constructed = _spy_evidence_construction(monkeypatch)
    submission = _submission(
        context,
        (
            _metric_assessment(segregation, 75),
            _metric_assessment(coupling, 30),
        ),
        confidence=79,
    )

    with pytest.raises(
        SemanticAssessmentInadmissibleError,
        match="policy_assessment_inadmissible",
    ) as exc_info:
        record_semantic_guideline_assessment(
            submission,
            context,
            receipt_id="receipt-inadmissible",
            recorded_at=NOW,
        )
    assert exc_info.value.code == "policy_assessment_inadmissible"
    assert (
        exc_info.value.cause
        == SemanticAssessmentInadmissibilityCause.CONFIDENCE_BELOW_MINIMUM.value
    )
    assert constructed == []


@pytest.mark.parametrize(
    "last_semantic_editor_id",
    ("assessor-1", LEGACY_UNKNOWN_SEMANTIC_EDITOR_ID),
)
def test_blocking_assessor_separation_rejects_before_any_result_or_receipt(
    last_semantic_editor_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture(
        last_semantic_editor_id=last_semantic_editor_id,
    )
    constructed = _spy_evidence_construction(monkeypatch)
    submission = _submission(
        context,
        (
            _metric_assessment(segregation, 75),
            _metric_assessment(coupling, 30),
        ),
        assessor_agent_id="assessor-1",
    )

    with pytest.raises(
        SemanticAssessmentInadmissibleError,
        match="policy_assessment_inadmissible",
    ) as exc_info:
        record_semantic_guideline_assessment(
            submission,
            context,
            receipt_id="receipt-separation-rejected",
            recorded_at=NOW,
        )
    assert exc_info.value.code == "policy_assessment_inadmissible"
    assert (
        exc_info.value.cause
        == (
            SemanticAssessmentInadmissibilityCause
            .ASSESSOR_SEPARATION_REQUIRED.value
        )
    )
    assert constructed == []


@pytest.mark.parametrize(
    ("last_semantic_editor_id", "expected_independent"),
    (
        ("assessor-1", False),
        (LEGACY_UNKNOWN_SEMANTIC_EDITOR_ID, True),
    ),
)
def test_advisory_binding_allows_self_or_legacy_unknown_assessment(
    last_semantic_editor_id: str,
    expected_independent: bool,
) -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture(
        enforcement=GuidelineEnforcement.ADVISORY,
        last_semantic_editor_id=last_semantic_editor_id,
    )
    receipt = record_semantic_guideline_assessment(
        _submission(
            context,
            (
                _metric_assessment(segregation, 75),
                _metric_assessment(coupling, 30),
            ),
            assessor_agent_id="assessor-1",
        ),
        context,
        receipt_id="receipt-advisory-self",
        recorded_at=NOW,
    ).receipt

    assert receipt.enforcement is GuidelineEnforcement.ADVISORY
    assert receipt.assessor_independent is expected_independent
    assert receipt.state is SemanticAssessmentState.PASSED


@pytest.mark.parametrize(
    ("kind", "error_code"),
    (
        ("missing", "semantic_assessment_metric_results_incomplete"),
        ("unknown", "semantic_assessment_metric_result_unknown"),
        ("not_applicable", "semantic_assessment_metric_result_not_applicable"),
    ),
)
def test_admission_rejects_non_exact_metric_sets(
    kind: str,
    error_code: str,
) -> None:
    segregation, coupling, refinement_only, _, _, context = _semantic_fixture()
    if kind == "missing":
        metric_results = (_metric_assessment(segregation, 80),)
    elif kind == "unknown":
        metric_results = (
            _metric_assessment(segregation, 80),
            _metric_assessment(coupling, 20),
            SemanticMetricAssessment(
                metric_id="metric-unknown",
                score=80,
                rationale="Unknown evidence.",
                evidence_refs=(_evidence(),),
                pinpoints=(_pinpoint(),),
            ),
        )
    else:
        metric_results = (
            _metric_assessment(segregation, 80),
            _metric_assessment(coupling, 20),
            _metric_assessment(refinement_only, 80),
        )
    submission = _submission(context, metric_results)

    with pytest.raises(SemanticAssessmentContractError, match=error_code):
        record_semantic_guideline_assessment(
            submission,
            context,
            receipt_id="receipt-invalid-set",
            recorded_at=NOW,
        )


def test_submission_rejects_duplicate_metric_before_admission() -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    duplicated = _metric_assessment(segregation, 80)
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_assessment_metric_result_duplicate",
    ):
        _submission(
            context,
            (
                duplicated,
                duplicated,
                _metric_assessment(coupling, 20),
            ),
        )


@pytest.mark.parametrize(
    ("override", "error_code"),
    (
        ({"score": True}, "semantic_metric_assessment_score_invalid"),
        ({"score": 1.5}, "semantic_metric_assessment_score_invalid"),
        ({"score": -1}, "semantic_metric_assessment_score_invalid"),
        ({"score": 101}, "semantic_metric_assessment_score_invalid"),
        ({"rationale": " "}, "semantic_metric_assessment_rationale_required"),
        ({"evidence_refs": ()}, "semantic_metric_assessment_evidence_refs_invalid"),
        ({"pinpoints": ()}, "semantic_metric_assessment_pinpoints_invalid"),
    ),
)
def test_metric_evidence_is_required_for_passes_and_failures(
    override: dict[str, object],
    error_code: str,
) -> None:
    values: dict[str, object] = {
        "metric_id": "metric-1",
        "score": 50,
        "rationale": "Complete rationale.",
        "evidence_refs": (_evidence(),),
        "pinpoints": (_pinpoint(),),
    }
    values.update(override)
    with pytest.raises(SemanticAssessmentContractError, match=error_code):
        SemanticMetricAssessment(**values)


@pytest.mark.parametrize("confidence", (True, -1, 101, 80.5))
def test_confidence_is_reserved_integer_zero_through_one_hundred(
    confidence: object,
) -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_assessment_confidence_invalid",
    ):
        _submission(
            context,
            (
                _metric_assessment(segregation, 80),
                _metric_assessment(coupling, 20),
            ),
            confidence=confidence,
        )


@pytest.mark.parametrize(
    ("fence", "error_code"),
    (
        ("subject", "semantic_assessment_subject_stale"),
        ("binding", "semantic_assessment_binding_stale"),
        ("revision", "semantic_assessment_guideline_revision_stale"),
    ),
)
def test_admission_fails_closed_on_stale_exact_fences(
    fence: str,
    error_code: str,
) -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    submission = _submission(
        context,
        (
            _metric_assessment(segregation, 80),
            _metric_assessment(coupling, 20),
        ),
    )
    if fence == "subject":
        submission = replace(
            submission,
            subject=replace(submission.subject, subject_version=5),
        )
    elif fence == "binding":
        submission = replace(
            submission,
            expected_binding_revision=2,
        )
    else:
        submission = replace(
            submission,
            guideline_revision_id="revision-other",
        )

    with pytest.raises(SemanticAssessmentContractError, match=error_code):
        record_semantic_guideline_assessment(
            submission,
            context,
            receipt_id="receipt-stale",
            recorded_at=NOW,
        )


def test_submission_order_is_canonical_and_no_composite_score_is_emitted() -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    first = _submission(
        context,
        (
            _metric_assessment(segregation, 80),
            _metric_assessment(coupling, 20),
        ),
    )
    second = _submission(
        context,
        (
            _metric_assessment(coupling, 20),
            _metric_assessment(segregation, 80),
        ),
    )

    assert semantic_assessment_input_digest_v1(
        context
    ) == semantic_assessment_input_digest_v1(context)
    assert semantic_assessment_request_digest_v1(
        first,
        context,
    ) == semantic_assessment_request_digest_v1(second, context)
    receipt = record_semantic_guideline_assessment(
        first,
        context,
        receipt_id="receipt-no-composite",
        recorded_at=NOW,
    ).receipt
    assert "composite_score" not in {
        field.name for field in fields(type(receipt))
    }
    assert "weight" not in {
        field.name for field in fields(type(receipt.metric_results[0]))
    }


def test_model_metadata_is_audit_only_and_never_changes_currentness_digest() -> None:
    segregation, coupling, _, _, _, context = _semantic_fixture()
    results = (
        _metric_assessment(segregation, 80),
        _metric_assessment(coupling, 20),
    )
    model_one = _submission(
        context,
        results,
        model_id="model-one",
    )
    model_two = _submission(
        context,
        results,
        model_id="model-two",
    )

    normative_digest = semantic_assessment_input_digest_v1(context)
    assert semantic_assessment_input_digest_v1(context) == normative_digest
    assert semantic_assessment_request_digest_v1(
        model_one,
        context,
    ) != semantic_assessment_request_digest_v1(model_two, context)
    first_receipt = record_semantic_guideline_assessment(
        model_one,
        context,
        receipt_id="receipt-model-one",
        recorded_at=NOW,
    ).receipt
    second_receipt = record_semantic_guideline_assessment(
        model_two,
        context,
        receipt_id="receipt-model-two",
        recorded_at=NOW,
    ).receipt

    assert first_receipt.input_digest == second_receipt.input_digest
    assert first_receipt.input_digest == normative_digest
    assert first_receipt.request_digest != second_receipt.request_digest
    assert first_receipt.policy_set_digest == second_receipt.policy_set_digest
    assert (
        first_receipt.binding_configuration_digest
        == second_receipt.binding_configuration_digest
    )


def test_semantic_policy_and_binding_head_digests_use_v2_authority() -> None:
    _, _, _, revision, binding, _ = _semantic_fixture()
    binding_head = semantic_binding_head_digest_v1((binding,))
    policy_set = semantic_policy_set_digest_v1(
        (binding,),
        (revision,),
    )
    stricter_binding = _binding(
        revision,
        minimum_confidence=90,
    )

    assert binding_head == semantic_binding_head_digest_v1([binding])
    assert policy_set == semantic_policy_set_digest_v1(
        [binding],
        [revision],
    )
    assert binding_head != semantic_binding_head_digest_v1(
        (stricter_binding,)
    )
    # Board configuration changes do not mutate the adopted semantic policy set.
    assert policy_set == semantic_policy_set_digest_v1(
        (stricter_binding,),
        (revision,),
    )


def test_pinpoint_reuses_quality_stability_rules() -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="finding_mutable_index_anchor_forbidden",
    ):
        UnboundFindingAnchor(
            anchor_type=FindingAnchorType.STRUCTURED_CHILD,
            anchor_ref="technical_requirements[0]",
        )
