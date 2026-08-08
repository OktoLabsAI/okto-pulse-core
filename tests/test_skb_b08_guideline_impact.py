"""Semantic guideline impact, adoption, unlink and retirement contracts."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.guideline_impact import (
    GUIDELINE_ADOPTION_ACTIVITY_ACTION,
    GUIDELINE_ADOPTION_EVENT_TYPE,
    GUIDELINE_RETIREMENT_ACTIVITY_ACTION,
    GUIDELINE_RETIREMENT_EVENT_TYPE,
    GUIDELINE_UNLINK_ACTIVITY_ACTION,
    GuidelineImpactCurrentnessReason,
    GuidelineImpactError,
    GuidelineImpactPreviewCommand,
    assess_guideline_impact_currentness,
    guideline_impact_preview_request_digest_v1,
    impact_fence_from_receipt,
    plan_guideline_adoption,
    plan_guideline_impact_preview,
    plan_guideline_retirement_impact,
    plan_guideline_unlink,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_IMPACT_CONTRACT_VERSION,
    BoardGuidelineBinding,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelineRetirement,
    GuidelineRevision,
    PolicyCurrentness,
    PolicyEntityType,
    PolicySubjectRef,
    PolicySubjectSnapshot,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentSubmission,
    SemanticMetricAssessment,
    record_semantic_guideline_assessment,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticMetricWaiver,
    SemanticMetricWaiverAnchor,
    request_semantic_metric_waiver,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    project_semantic_metric_findings,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    UnboundFindingAnchor,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)


def _metric(
    metric_id: str = "metric-segregation",
    *,
    code: str = "segregation",
    description: str = "Measures separation of technical and business concerns.",
    rubric: str = "0 means mixed concerns; 100 means complete isolation.",
    targets: tuple[PolicyEntityType, ...] = (PolicyEntityType.SPEC,),
    direction: GuidelineMetricDirection = GuidelineMetricDirection.MINIMUM,
    threshold: int = 80,
) -> GuidelineMetric:
    return GuidelineMetric(
        metric_id=metric_id,
        code=code,
        title=code.replace("_", " ").title(),
        description=description,
        evaluation_rubric=rubric,
        target_entity_types=targets,
        direction=direction,
        default_threshold=threshold,
    )


def _revision(
    number: int,
    *,
    guideline_id: str = "guideline-1",
    metrics: tuple[GuidelineMetric, ...] | None = None,
) -> GuidelineRevision:
    return GuidelineRevision(
        revision_id=f"{guideline_id}-revision-{number}",
        guideline_id=guideline_id,
        revision_number=number,
        semantic_version=("1.0.0" if number == 1 else f"1.{number - 1}.0"),
        title="Hexagonal architecture",
        content=f"Semantic revision {number}.",
        metrics=metrics if metrics is not None else (_metric(),),
        created_by="author-1",
        created_at=NOW + timedelta(minutes=number),
        parent_revision_id=(
            None if number == 1 else f"{guideline_id}-revision-{number - 1}"
        ),
    )


def _head(revision: GuidelineRevision) -> GuidelineHead:
    return GuidelineHead(
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        revision_number=revision.revision_number,
        semantic_version=revision.semantic_version,
        head_revision=revision.revision_number,
        updated_at=revision.created_at,
    )


def _binding(
    revision: GuidelineRevision,
    *,
    binding_revision: int = 1,
    priority: int = 10,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.ADVISORY,
    minimum_confidence: int = 70,
    overrides: dict[str, int] | None = None,
) -> BoardGuidelineBinding:
    return BoardGuidelineBinding(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        semantic_version=revision.semantic_version,
        revision_digest=revision.revision_digest,
        priority=priority,
        binding_revision=binding_revision,
        adopted_by="actor-before",
        adopted_at=NOW,
        enforcement=enforcement,
        minimum_confidence=minimum_confidence,
        metric_threshold_overrides=overrides or {},
        source_kind=GuidelineBindingProvenance.NATIVE,
    )


def _preview(
    current: GuidelineRevision,
    target: GuidelineRevision,
    binding: BoardGuidelineBinding,
    *,
    priority: int = 20,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
    minimum_confidence: int = 85,
    overrides: dict[str, int] | None = None,
    waivers: tuple[SemanticMetricWaiver, ...] = (),
) -> object:
    return plan_guideline_impact_preview(
        GuidelineImpactPreviewCommand(
            impact_receipt_id="impact-1",
            board_id=binding.board_id,
            guideline_id=binding.guideline_id,
            head=_head(target),
            to_revision=target,
            current_binding=binding,
            from_revision=current,
            active_bindings=(binding,),
            active_revisions=(current,),
            subjects=(
                PolicySubjectRef(
                    board_id=binding.board_id,
                    entity_type=PolicyEntityType.SPEC,
                    subject_id="spec-1",
                    subject_version=3,
                ),
            ),
            waivers=waivers,
            proposed_priority=priority,
            proposed_enforcement=enforcement,
            proposed_minimum_confidence=minimum_confidence,
            proposed_metric_threshold_overrides=overrides or {},
            requested_by="agent-1",
            created_at=NOW + timedelta(minutes=10),
            idempotency_key="preview:1",
            requested_to_revision_id=target.revision_id,
        )
    )


def _requested_semantic_waiver(
    revision: GuidelineRevision,
    binding: BoardGuidelineBinding,
) -> SemanticMetricWaiver:
    subject = PolicySubjectRef(
        board_id=binding.board_id,
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        subject_version=3,
    )
    content_digest = "a" * 64
    evidence = EvidenceRef(
        source_type="spec",
        source_id=subject.subject_id,
        source_version=subject.subject_version,
        content_hash=content_digest,
    )
    assessment = record_semantic_guideline_assessment(
        SemanticGuidelineAssessmentSubmission(
            subject=subject,
            binding_id=binding.binding_id,
            expected_binding_revision=binding.binding_revision,
            guideline_revision_id=revision.revision_id,
            idempotency_key="impact-waiver-assessment",
            confidence=90,
            assessor=SemanticAssessmentAssessor(
                agent_id="reviewer-1",
                model_id="model-a",
            ),
            metric_results=(
                SemanticMetricAssessment(
                    metric_id="metric-segregation",
                    score=60,
                    rationale="The current spec mixes domain and adapter concerns.",
                    evidence_refs=(evidence,),
                    pinpoints=(
                        UnboundFindingAnchor(
                            anchor_type=FindingAnchorType.STRUCTURED_CHILD,
                            anchor_ref=(
                                "technical_requirements.metric-segregation"
                            ),
                            excerpt_hash="b" * 64,
                        ),
                    ),
                ),
            ),
        ),
        SemanticGuidelineAssessmentContext(
            subject_snapshot=PolicySubjectSnapshot(
                subject=subject,
                content_digest=content_digest,
                last_semantic_editor_id="editor-1",
                captured_at=NOW,
            ),
            binding=binding,
            revision=revision,
            policy_set_digest="c" * 64,
            binding_head_digest="d" * 64,
        ),
        receipt_id="receipt-impact-waiver",
        recorded_at=NOW + timedelta(minutes=2),
    )
    findings = project_semantic_metric_findings(assessment.receipt)
    assert len(findings) == 1
    return request_semantic_metric_waiver(
        waiver_id="waiver-impact-1",
        event_id="waiver-impact-event-1",
        anchor=SemanticMetricWaiverAnchor.from_finding(
            findings[0],
            assessment_assessor_id=assessment.receipt.assessor.agent_id,
        ),
        justification="Temporary migration exception pending adapter extraction.",
        evidence_refs=(evidence,),
        requested_by="requester-1",
        requested_at=NOW + timedelta(minutes=3),
        expires_at=NOW + timedelta(days=7),
        idempotency_key="impact-waiver-request",
    ).waiver


def test_preview_seals_metric_deltas_and_exact_board_configuration() -> None:
    current = _revision(1)
    target = _revision(
        2,
        metrics=(
            _metric(description="Revised semantic definition."),
            _metric(
                "metric-dependency",
                code="dependency_direction",
                description="Measures inward dependency direction.",
            ),
        ),
    )
    preview = _preview(
        current,
        target,
        _binding(current),
        overrides={"segregation": 90},
    )

    assert GUIDELINE_IMPACT_CONTRACT_VERSION == "guideline-impact/v2"
    assert preview.receipt.added_metric_ids == ("metric-dependency",)
    assert preview.receipt.changed_metric_ids == ("metric-segregation",)
    assert preview.receipt.removed_metric_ids == ()
    assert preview.receipt.proposed_enforcement is GuidelineEnforcement.BLOCKING
    assert preview.receipt.proposed_minimum_confidence == 85
    assert dict(preview.receipt.proposed_metric_threshold_overrides) == {
        "segregation": 90
    }
    assert preview.proposed_binding.enforcement is GuidelineEnforcement.BLOCKING
    assert preview.proposed_binding.minimum_confidence == 85
    assert dict(preview.proposed_binding.metric_threshold_overrides) == {
        "segregation": 90
    }
    assert preview.receipt.items[0].item_kind.value == "binding"
    assert {item.entity_id for item in preview.receipt.items} >= {
        "board-1",
        "spec",
        "spec-1",
    }
    with pytest.raises(TypeError):
        preview.proposed_binding.metric_threshold_overrides["segregation"] = 1


def test_preview_seals_semantic_waiver_metric_lineage_without_rule_aliases() -> None:
    current = _revision(1)
    target = _revision(
        2,
        metrics=(_metric(description="Revised semantic definition."),),
    )
    binding = _binding(current)
    waiver = _requested_semantic_waiver(current, binding)

    preview = _preview(
        current,
        target,
        binding,
        waivers=(waiver,),
    )
    waiver_items = tuple(
        item
        for item in preview.receipt.items
        if item.item_kind.value == "waiver"
    )

    assert len(waiver_items) == 1
    assert waiver_items[0].related_id == waiver.waiver_id
    anchor = waiver.anchor
    expected_details = {
        "waiver_id": waiver.waiver_id,
        "waiver_revision": waiver.waiver_revision,
        "status": waiver.status.value,
        "scope_digest": waiver.scope_digest,
        "head_digest": waiver.head_digest,
        "receipt_id": anchor.receipt_id,
        "receipt_digest": anchor.receipt_digest,
        "finding_id": anchor.finding_id,
        "finding_digest": anchor.finding_digest,
        "guideline_id": anchor.guideline_id,
        "revision_id": anchor.guideline_revision_id,
        "revision_digest": anchor.guideline_revision_digest,
        "binding_id": anchor.binding_id,
        "binding_revision": anchor.binding_revision,
        "binding_configuration_digest": anchor.binding_configuration_digest,
        "metric_id": anchor.metric_id,
        "metric_code": anchor.metric_code,
        "metric_result_id": anchor.metric_result_id,
        "metric_result_digest": anchor.metric_result_digest,
        "subject_type": anchor.subject.entity_type.value,
        "subject_id": anchor.subject.subject_id,
        "subject_version": anchor.subject.subject_version,
        "subject_content_digest": anchor.subject_content_digest,
        "expires_at": waiver.expires_at.isoformat(),
        "requires_revalidation": True,
        "policy_set_digest_after": preview.receipt.policy_set_digest_after,
    }
    assert waiver_items[0].details_digest == canonical_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "kind": "impact_item_details",
            **expected_details,
        }
    )
    assert "rule_id" not in expected_details


def test_revision_only_change_surfaces_staled_subjects_and_waivers() -> None:
    current = _revision(1)
    target = _revision(2, metrics=current.metrics)
    binding = _binding(current)
    waiver = _requested_semantic_waiver(current, binding)

    preview = _preview(
        current,
        target,
        binding,
        priority=binding.priority,
        enforcement=binding.enforcement,
        minimum_confidence=binding.minimum_confidence,
        overrides=dict(binding.metric_threshold_overrides),
        waivers=(waiver,),
    )

    assert preview.receipt.added_metric_ids == ()
    assert preview.receipt.changed_metric_ids == ()
    assert preview.receipt.removed_metric_ids == ()
    assert {
        (item.item_kind.value, item.entity_id)
        for item in preview.receipt.items
    } >= {
        ("artifact", "spec-1"),
        ("waiver", "spec-1"),
    }


def test_preview_rejects_unknown_override_and_exact_noop() -> None:
    revision = _revision(1)
    binding = _binding(
        revision,
        priority=10,
        enforcement=GuidelineEnforcement.BLOCKING,
        minimum_confidence=80,
        overrides={"segregation": 90},
    )
    with pytest.raises(
        GuidelineImpactError,
        match="guideline_impact_threshold_override_unknown",
    ):
        _preview(revision, revision, binding, overrides={"unknown": 80})

    with pytest.raises(
        GuidelineImpactError,
        match="guideline_impact_no_changes",
    ):
        _preview(
            revision,
            revision,
            binding,
            priority=10,
            enforcement=GuidelineEnforcement.BLOCKING,
            minimum_confidence=80,
            overrides={"segregation": 90},
        )


def test_preview_request_digest_binds_all_semantic_configuration() -> None:
    base = dict(
        board_id="board-1",
        guideline_id="guideline-1",
        proposed_priority=10,
        proposed_enforcement=GuidelineEnforcement.BLOCKING,
        proposed_minimum_confidence=80,
        proposed_metric_threshold_overrides={"segregation": 90},
        requested_by="agent-1",
        requested_to_revision_id="guideline-1-revision-2",
    )
    digest = guideline_impact_preview_request_digest_v1(**base)
    assert digest != guideline_impact_preview_request_digest_v1(
        **{**base, "proposed_minimum_confidence": 81}
    )
    assert digest != guideline_impact_preview_request_digest_v1(
        **{
            **base,
            "proposed_metric_threshold_overrides": {"segregation": 91},
        }
    )


def test_currentness_and_adoption_fail_closed_on_stale_fence() -> None:
    current = _revision(1)
    target = _revision(2)
    binding = _binding(current)
    preview = _preview(current, target, binding)
    fence = impact_fence_from_receipt(preview.receipt)

    assessment = assess_guideline_impact_currentness(preview.receipt, fence)
    assert assessment.currentness is PolicyCurrentness.CURRENT
    assert assessment.reasons == ()

    stale = replace(fence, binding_digest="f" * 64)
    assessment = assess_guideline_impact_currentness(preview.receipt, stale)
    assert assessment.currentness is PolicyCurrentness.STALE
    assert assessment.reasons == (
        GuidelineImpactCurrentnessReason.BINDING_CHANGED,
    )
    with pytest.raises(
        GuidelineImpactError,
        match="guideline_impact_stale",
    ):
        plan_guideline_adoption(
            receipt=preview.receipt,
            current_snapshot=stale,
            current_binding=binding,
            retirement=None,
            actor_id="agent-1",
            actor_type="agent",
            occurred_at=NOW + timedelta(minutes=11),
            event_id="event-adopt-stale",
            idempotency_key="adopt:stale",
        )


def test_adoption_materializes_exact_config_and_metric_lineage() -> None:
    current = _revision(1)
    target = _revision(
        2,
        metrics=(
            _metric(description="Updated definition."),
            _metric("metric-dependency", code="dependency_direction"),
        ),
    )
    binding = _binding(current)
    preview = _preview(
        current,
        target,
        binding,
        overrides={"segregation": 92},
    )
    mutation = plan_guideline_adoption(
        receipt=preview.receipt,
        current_snapshot=impact_fence_from_receipt(preview.receipt),
        current_binding=binding,
        retirement=None,
        actor_id="agent-1",
        actor_type="agent",
        occurred_at=NOW + timedelta(minutes=11),
        event_id="event-adopt-1",
        idempotency_key="adopt:1",
    )

    assert mutation.activity_action == GUIDELINE_ADOPTION_ACTIVITY_ACTION
    assert mutation.event.event_type == GUIDELINE_ADOPTION_EVENT_TYPE
    assert mutation.event.added_metric_ids == ("metric-dependency",)
    assert mutation.event.changed_metric_ids == ("metric-segregation",)
    assert mutation.event.removed_metric_ids == ()
    assert mutation.binding.binding_revision == binding.binding_revision + 1
    assert mutation.binding.enforcement is GuidelineEnforcement.BLOCKING
    assert mutation.binding.minimum_confidence == 85
    assert dict(mutation.binding.metric_threshold_overrides) == {
        "segregation": 92
    }
    payload = mutation.event.payload()
    assert payload["added_metric_ids"] == ("metric-dependency",)
    assert "added_rule_ids" not in payload


def test_unlink_preserves_config_on_tombstone_and_removes_metric_ids() -> None:
    revision = _revision(
        1,
        metrics=(
            _metric(),
            _metric("metric-dependency", code="dependency_direction"),
        ),
    )
    binding = _binding(
        revision,
        enforcement=GuidelineEnforcement.BLOCKING,
        minimum_confidence=88,
        overrides={"segregation": 91},
    )
    mutation = plan_guideline_unlink(
        current_binding=binding,
        current_revision=revision,
        active_bindings=(binding,),
        active_revisions=(revision,),
        retirement=None,
        actor_id="agent-1",
        actor_type="agent",
        occurred_at=NOW + timedelta(minutes=20),
        event_id="event-unlink-1",
        idempotency_key="unlink:1",
    )

    assert mutation.activity_action == GUIDELINE_UNLINK_ACTIVITY_ACTION
    assert mutation.binding.state is GuidelineBindingState.UNLINKED
    assert mutation.binding.enforcement is GuidelineEnforcement.BLOCKING
    assert mutation.binding.minimum_confidence == 88
    assert dict(mutation.binding.metric_threshold_overrides) == {
        "segregation": 91
    }
    assert mutation.removed_metric_ids == (
        "metric-dependency",
        "metric-segregation",
    )
    assert mutation.event.removed_metric_ids == mutation.removed_metric_ids


def test_retirement_impact_uses_semantic_metric_lineage() -> None:
    revision = _revision(
        1,
        metrics=(
            _metric(),
            _metric("metric-dependency", code="dependency_direction"),
        ),
    )
    binding = _binding(revision)
    retirement = GuidelineRetirement(
        retirement_id="retirement-1",
        guideline_id=revision.guideline_id,
        status=GuidelineLifecycleStatus.RETIRED,
        retired_revision_id=revision.revision_id,
        retired_revision_number=revision.revision_number,
        retired_semantic_version=revision.semantic_version,
        retired_revision_digest=revision.revision_digest,
        retired_head_revision=revision.revision_number,
        reason="Superseded operational guidance.",
        retired_by="agent-1",
        retired_at=NOW + timedelta(minutes=30),
    )
    mutation = plan_guideline_retirement_impact(
        retirement=retirement,
        current_binding=binding,
        current_revision=revision,
        active_bindings=(binding,),
        active_revisions=(revision,),
        actor_type="agent",
        request_digest="c" * 64,
    )

    assert mutation.activity_action == GUIDELINE_RETIREMENT_ACTIVITY_ACTION
    assert mutation.event.event_type == GUIDELINE_RETIREMENT_EVENT_TYPE
    assert mutation.event.removed_metric_ids == (
        "metric-dependency",
        "metric-segregation",
    )
    assert "removed_rule_ids" not in mutation.event.payload()
    assert mutation.impact_digest
