"""SK-B B08 adversarial contracts for preview, adoption, and unlink lineage."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.guideline_impact import (
    GUIDELINE_ADOPTION_ACTIVITY_ACTION,
    GUIDELINE_RETIREMENT_ACTIVITY_ACTION,
    GUIDELINE_RETIREMENT_EVENT_TYPE,
    GUIDELINE_UNLINK_ACTIVITY_ACTION,
    GuidelineImpactCurrentnessReason,
    GuidelineImpactError,
    GuidelineImpactPreviewCommand,
    guideline_impact_preview_request_digest_v1,
    assess_guideline_impact_currentness,
    impact_fence_from_receipt,
    plan_guideline_adoption,
    plan_guideline_impact_preview,
    plan_guideline_retirement_impact,
    plan_guideline_unlink,
)
from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineImpactItemKind,
    GuidelineLifecycleStatus,
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRetirement,
    GuidelineRevision,
    GuidelineRule,
    PolicyCurrentness,
    PolicyEntityType,
    PolicySubjectRef,
    PolicyWaiver,
    PolicyWaiverEventType,
    PolicyWaiverStatus,
)
from okto_pulse.core.domain.guideline_policy_evaluator import (
    policy_binding_head_digest_v1,
    policy_set_digest_v1,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelineImpactPreviewReplay,
    GuidelinePolicyCasConflict,
    GuidelinePolicyIdempotencyConflict,
)
from okto_pulse.core.services.main import GuidelineService


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)


def _rule(
    rule_id: str = "rule-1",
    *,
    title: str | None = None,
    targets: tuple[PolicyEntityType, ...] = (PolicyEntityType.SPEC,),
) -> GuidelineRule:
    return GuidelineRule(
        rule_id=rule_id,
        code=f"policy.{rule_id}",
        title=title or rule_id,
        description=f"Deterministic {rule_id}.",
        target_entity_types=targets,
        predicates=(
            GuidelinePredicate(
                "gte",
                (("fact", "coverage_percent"), ("value", 100)),
            ),
        ),
    )


def _revision(
    number: int,
    *,
    guideline_id: str = "guideline-1",
    rules: tuple[GuidelineRule, ...] | None = None,
) -> GuidelineRevision:
    return GuidelineRevision(
        revision_id=f"{guideline_id}-revision-{number}",
        guideline_id=guideline_id,
        revision_number=number,
        semantic_version=f"1.{number - 1}.0",
        title=f"Policy {guideline_id}",
        content=f"Revision {number}.",
        content_digest=f"{number:x}" * 64,
        rules=rules if rules is not None else (_rule(),),
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
    binding_id: str = "binding-1",
    board_id: str = "board-1",
    priority: int = 10,
    binding_revision: int = 1,
    source_kind: GuidelineBindingProvenance = GuidelineBindingProvenance.NATIVE,
) -> BoardGuidelineBinding:
    return BoardGuidelineBinding(
        binding_id=binding_id,
        board_id=board_id,
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        semantic_version=revision.semantic_version,
        revision_digest=revision.content_digest,
        priority=priority,
        binding_revision=binding_revision,
        adopted_by="actor-before",
        adopted_at=NOW,
        default_enforcement=GuidelineEnforcement.ADVISORY,
        source_kind=source_kind,
    )


def _preview(
    *,
    current_revision: GuidelineRevision,
    target_revision: GuidelineRevision,
    head_revision: GuidelineRevision,
    current_binding: BoardGuidelineBinding,
    subjects: tuple[PolicySubjectRef, ...] = (),
    waivers: tuple[PolicyWaiver, ...] = (),
    requested_to_revision_id: str | None = None,
) -> object:
    return plan_guideline_impact_preview(
        GuidelineImpactPreviewCommand(
            impact_receipt_id="impact-1",
            board_id=current_binding.board_id,
            guideline_id=current_binding.guideline_id,
            head=_head(head_revision),
            to_revision=target_revision,
            current_binding=current_binding,
            from_revision=current_revision,
            active_bindings=(current_binding,),
            active_revisions=(current_revision,),
            subjects=subjects,
            waivers=waivers,
            proposed_priority=current_binding.priority,
            proposed_default_enforcement=current_binding.default_enforcement,
            requested_by="agent-1",
            created_at=NOW + timedelta(minutes=10),
            idempotency_key="preview:1",
            requested_to_revision_id=requested_to_revision_id,
        )
    )


def _subject(
    entity_type: PolicyEntityType,
    subject_id: str,
    *,
    version: int = 1,
) -> PolicySubjectRef:
    return PolicySubjectRef(
        board_id="board-1",
        entity_type=entity_type,
        subject_id=subject_id,
        subject_version=version,
    )


def _waiver(
    waiver_id: str,
    *,
    guideline_id: str,
    subject: PolicySubjectRef,
    rule_id: str = "rule-1",
) -> PolicyWaiver:
    return PolicyWaiver(
        waiver_id=waiver_id,
        board_id=subject.board_id,
        finding_id=f"finding-{waiver_id}",
        receipt_id=f"receipt-{waiver_id}",
        guideline_id=guideline_id,
        revision_id=f"{guideline_id}-revision-1",
        rule_id=rule_id,
        subject=subject,
        status=PolicyWaiverStatus.REQUESTED,
        justification="Bounded migration exception.",
        evidence_refs=(f"ticket://{waiver_id}",),
        requested_by="requester-1",
        requested_at=NOW,
        waiver_revision=1,
        expires_at=NOW + timedelta(days=7),
        last_event_id=f"event-{waiver_id}",
        last_event_type=PolicyWaiverEventType.REQUEST,
        last_event_at=NOW,
    )


def _historical_adoption():
    revision_1 = _revision(1)
    revision_2 = _revision(2, rules=(_rule(), _rule("rule-2")))
    revision_3 = _revision(
        3,
        rules=(_rule(title="Head changed"), _rule("rule-2")),
    )
    binding = _binding(revision_1)
    preview = _preview(
        current_revision=revision_1,
        target_revision=revision_2,
        head_revision=revision_3,
        current_binding=binding,
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
    return revision_1, revision_2, revision_3, binding, preview, mutation


@pytest.mark.asyncio
async def test_preview_replay_treats_omitted_target_as_exact_not_wildcard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revision_1 = _revision(1)
    revision_2 = _revision(2, rules=(_rule(), _rule("rule-2")))
    live_head = _revision(3, rules=(_rule(), _rule("rule-2"), _rule("rule-3")))
    binding = _binding(revision_1)
    original = _preview(
        current_revision=revision_1,
        target_revision=revision_2,
        head_revision=live_head,
        current_binding=binding,
        requested_to_revision_id=revision_2.revision_id,
    )
    replay = GuidelineImpactPreviewReplay(
        receipt=original.receipt,
        request_digest=original.request_digest,
    )

    class _ReplayPolicy:
        async def get_impact_receipt_by_idempotency(self, **_kwargs):
            return replay

    service = GuidelineService(object())
    monkeypatch.setattr(service, "_policy", lambda: _ReplayPolicy())

    omitted_digest = guideline_impact_preview_request_digest_v1(
        board_id="board-1",
        guideline_id="guideline-1",
        proposed_priority=binding.priority,
        proposed_default_enforcement=binding.default_enforcement,
        requested_by="agent-1",
        requested_to_revision_id=None,
    )
    assert omitted_digest != replay.request_digest
    with pytest.raises(
        GuidelinePolicyIdempotencyConflict,
        match="guideline_impact_idempotency_payload_mismatch",
    ):
        await service.preview_guideline_revision_impact(
            board_id="board-1",
            guideline_id="guideline-1",
            proposed_priority=binding.priority,
            proposed_default_enforcement=binding.default_enforcement,
            requested_by="agent-1",
            idempotency_key="preview:1",
            to_revision_id=None,
        )

    exact = await service.preview_guideline_revision_impact(
        board_id="board-1",
        guideline_id="guideline-1",
        proposed_priority=binding.priority,
        proposed_default_enforcement=binding.default_enforcement,
        requested_by="agent-1",
        idempotency_key="preview:1",
        to_revision_id=revision_2.revision_id,
    )
    assert exact == original.receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "replan_error",
    ("guideline_is_terminal", "guideline_impact_no_changes"),
)
async def test_adoption_replan_state_changes_are_closed_cas_conflicts(
    monkeypatch: pytest.MonkeyPatch,
    replan_error: str,
) -> None:
    revision_1 = _revision(1)
    revision_2 = _revision(2, rules=(_rule(), _rule("rule-2")))
    binding = _binding(revision_1)
    receipt = _preview(
        current_revision=revision_1,
        target_revision=revision_2,
        head_revision=revision_2,
        current_binding=binding,
    ).receipt

    class _Policy:
        async def get_adoption_result_by_idempotency(self, **_kwargs):
            return None

        async def get_impact_receipt(self, **_kwargs):
            return receipt

    async def _raise_replan_error(**_kwargs):
        raise GuidelineImpactError(replan_error)

    service = GuidelineService(object())
    monkeypatch.setattr(service, "_policy", lambda: _Policy())
    monkeypatch.setattr(
        service,
        "_build_guideline_impact_plan",
        _raise_replan_error,
    )

    with pytest.raises(GuidelinePolicyCasConflict, match=replan_error):
        await service.adopt_guideline_revision(
            board_id=receipt.board_id,
            guideline_id=receipt.guideline_id,
            impact_receipt_id=receipt.impact_receipt_id,
            impact_digest=receipt.impact_digest,
            actor_id="agent-1",
            actor_type="agent",
            idempotency_key=f"adopt:{replan_error}",
        )


def test_preview_rejects_an_effect_free_noop() -> None:
    revision = _revision(1)
    binding = _binding(revision)

    with pytest.raises(
        GuidelineImpactError,
        match="guideline_impact_no_changes",
    ):
        _preview(
            current_revision=revision,
            target_revision=revision,
            head_revision=revision,
            current_binding=binding,
        )


def test_preview_lists_and_fences_only_affected_artifacts_and_waivers() -> None:
    current = _revision(1)
    target = _revision(
        2,
        rules=(_rule(title="Changed executable requirement"),),
    )
    binding = _binding(current)
    affected = _subject(PolicyEntityType.SPEC, "spec-1", version=3)
    unaffected = _subject(PolicyEntityType.CARD, "card-1", version=7)
    affected_waiver = _waiver(
        "waiver-affected",
        guideline_id=current.guideline_id,
        subject=affected,
    )
    unrelated_guideline_waiver = _waiver(
        "waiver-other-guideline",
        guideline_id="guideline-2",
        subject=affected,
    )
    unrelated_target_waiver = _waiver(
        "waiver-other-target",
        guideline_id=current.guideline_id,
        subject=unaffected,
    )
    absent_subject_waiver = _waiver(
        "waiver-absent-subject",
        guideline_id=current.guideline_id,
        subject=_subject(PolicyEntityType.SPEC, "spec-absent"),
    )

    receipt = _preview(
        current_revision=current,
        target_revision=target,
        head_revision=target,
        current_binding=binding,
        subjects=(unaffected, affected),
        waivers=(
            unrelated_target_waiver,
            absent_subject_waiver,
            affected_waiver,
            unrelated_guideline_waiver,
        ),
    ).receipt
    shuffled = _preview(
        current_revision=current,
        target_revision=target,
        head_revision=target,
        current_binding=binding,
        subjects=(affected, unaffected),
        waivers=(
            unrelated_guideline_waiver,
            affected_waiver,
            absent_subject_waiver,
            unrelated_target_waiver,
        ),
    ).receipt

    assert shuffled == receipt
    assert tuple(
        (item.entity_type, item.entity_id)
        for item in receipt.items
        if item.item_kind is GuidelineImpactItemKind.ARTIFACT
    ) == (("spec", "spec-1"),)
    assert tuple(
        item.related_id
        for item in receipt.items
        if item.item_kind is GuidelineImpactItemKind.WAIVER
    ) == ("waiver-affected",)

    irrelevant_changes = _preview(
        current_revision=current,
        target_revision=target,
        head_revision=target,
        current_binding=binding,
        subjects=(
            affected,
            replace(unaffected, subject_version=99),
        ),
        waivers=(affected_waiver,),
    ).receipt
    assert irrelevant_changes.artifact_snapshot_digest == (
        receipt.artifact_snapshot_digest
    )
    assert irrelevant_changes.waiver_snapshot_digest == (receipt.waiver_snapshot_digest)

    affected_change = _preview(
        current_revision=current,
        target_revision=target,
        head_revision=target,
        current_binding=binding,
        subjects=(
            replace(affected, subject_version=4),
            unaffected,
        ),
        waivers=(affected_waiver,),
    ).receipt
    assessment = assess_guideline_impact_currentness(
        receipt,
        impact_fence_from_receipt(affected_change),
    )
    assert assessment.currentness is PolicyCurrentness.STALE
    assert (
        GuidelineImpactCurrentnessReason.ARTIFACT_SNAPSHOT_CHANGED in assessment.reasons
    )


def test_historical_pin_is_allowed_then_becomes_stale_when_head_advances() -> None:
    _, revision_2, revision_3, binding, preview, mutation = _historical_adoption()

    assert preview.receipt.to_revision_id == revision_2.revision_id
    assert preview.receipt.expected_head_revision == revision_3.revision_number
    assert mutation.binding.revision_id == revision_2.revision_id
    assert mutation.binding.binding_revision == binding.binding_revision + 1

    advanced_fence = replace(
        impact_fence_from_receipt(preview.receipt),
        head_revision=revision_3.revision_number + 1,
    )
    assessment = assess_guideline_impact_currentness(
        preview.receipt,
        advanced_fence,
    )
    assert assessment.currentness is PolicyCurrentness.STALE
    assert assessment.reasons == (
        GuidelineImpactCurrentnessReason.GUIDELINE_HEAD_CHANGED,
    )

    with pytest.raises(
        GuidelineImpactError,
        match="guideline_impact_stale",
    ) as exc:
        plan_guideline_adoption(
            receipt=preview.receipt,
            current_snapshot=advanced_fence,
            current_binding=binding,
            retirement=None,
            actor_id="agent-1",
            actor_type="agent",
            occurred_at=NOW + timedelta(minutes=12),
            event_id="event-adopt-stale",
            idempotency_key="adopt:stale",
        )
    assert exc.value.currentness_reasons == (
        GuidelineImpactCurrentnessReason.GUIDELINE_HEAD_CHANGED,
    )


@pytest.mark.parametrize("invalid_flag", (False, 1, "true"))
def test_impact_receipt_requires_literal_true(
    invalid_flag: object,
) -> None:
    *_, preview, _ = _historical_adoption()

    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_impact_adoption_flag_invalid",
    ):
        replace(
            preview.receipt,
            requires_explicit_adoption=invalid_flag,
        )


def test_adoption_event_rejects_malformed_shape_and_rule_sets() -> None:
    *_, mutation = _historical_adoption()

    with pytest.raises(
        GuidelineImpactError,
        match="guideline_adoption_event_actor_type_invalid",
    ):
        replace(mutation.event, actor_type="robot")
    with pytest.raises(
        GuidelineImpactError,
        match="guideline_adoption_event_to_revision_invalid",
    ):
        replace(mutation.event, to_revision_digest=None)
    with pytest.raises(
        GuidelineImpactError,
        match="guideline_adoption_event_rule_sets_overlap",
    ):
        replace(
            mutation.event,
            changed_rule_ids=mutation.event.added_rule_ids,
        )


def test_adoption_mutation_rejects_bundle_tampering() -> None:
    *_, mutation = _historical_adoption()

    with pytest.raises(
        GuidelineImpactError,
        match="guideline_adoption_mutation_payload_invalid",
    ):
        replace(mutation, activity_action="not-adoption")
    event_tampering = (
        {
            "to_revision_id": "guideline-1-revision-99",
            "to_semantic_version": "9.9.9",
            "to_revision_digest": "9" * 64,
        },
        {"binding_digest_before": "8" * 64},
        {"binding_head_digest_before": "7" * 64},
        {"binding_head_digest_after": "6" * 64},
        {"policy_set_digest_before": "5" * 64},
        {"policy_set_digest_after": "4" * 64},
        {"added_rule_ids": ("rule-99",)},
        {"changed_rule_ids": ("rule-99",)},
        {"removed_rule_ids": ("rule-99",)},
    )
    for changes in event_tampering:
        with pytest.raises(
            GuidelineImpactError,
            match="guideline_adoption_mutation_payload_invalid",
        ):
            replace(
                mutation,
                event=replace(
                    mutation.event,
                    **changes,
                ),
            )


def test_unlink_planner_seals_event_activity_lineage_and_idempotency() -> None:
    current_revision = _revision(1, rules=(_rule(), _rule("rule-2")))
    current_binding = _binding(
        current_revision,
        source_kind=GuidelineBindingProvenance.DEFAULT_MATERIALIZATION,
    )
    other_revision = _revision(1, guideline_id="guideline-2")
    other_binding = _binding(
        other_revision,
        binding_id="binding-2",
        priority=1,
    )
    arguments = {
        "current_binding": current_binding,
        "current_revision": current_revision,
        "active_bindings": (current_binding, other_binding),
        "active_revisions": (current_revision, other_revision),
        "retirement": None,
        "actor_id": "agent-1",
        "actor_type": "agent",
        "occurred_at": NOW + timedelta(minutes=20),
        "event_id": "event-unlink-1",
        "idempotency_key": "unlink:1",
    }

    mutation = plan_guideline_unlink(**arguments)
    replay = plan_guideline_unlink(**arguments)

    assert replay == mutation
    assert mutation.idempotency_key == "unlink:1"
    assert mutation.binding.state is GuidelineBindingState.UNLINKED
    assert mutation.binding.source_kind is current_binding.source_kind
    assert mutation.binding.binding_revision == 2
    assert mutation.binding.revision_id == current_binding.revision_id
    assert mutation.activity_action == GUIDELINE_UNLINK_ACTIVITY_ACTION
    assert mutation.activity_action != GUIDELINE_ADOPTION_ACTIVITY_ACTION
    assert mutation.event.operation == "unlink"
    assert mutation.event.from_revision_id == current_binding.revision_id
    assert mutation.event.to_revision_id is None
    assert mutation.event.removed_rule_ids == ("rule-1", "rule-2")
    assert mutation.event.binding_head_digest_before == (
        policy_binding_head_digest_v1((other_binding, current_binding))
    )
    assert mutation.event.binding_head_digest_after == (
        policy_binding_head_digest_v1((other_binding,))
    )
    assert mutation.event.policy_set_digest_before == policy_set_digest_v1(
        (other_binding, current_binding),
        (other_revision, current_revision),
    )
    assert mutation.event.policy_set_digest_after == policy_set_digest_v1(
        (other_binding,),
        (other_revision,),
    )
    payload = mutation.event.payload()
    assert payload["policy_set_digest"] == payload["policy_set_digest_after"]
    assert payload["binding_revision"] == 2


def test_unlink_event_and_mutation_reject_lineage_tampering() -> None:
    revision = _revision(1)
    binding = _binding(revision)
    mutation = plan_guideline_unlink(
        current_binding=binding,
        current_revision=revision,
        active_bindings=(binding,),
        active_revisions=(revision,),
        retirement=None,
        actor_id="agent-1",
        actor_type="agent",
        occurred_at=NOW + timedelta(minutes=20),
        event_id="event-unlink-tamper",
        idempotency_key="unlink:tamper",
    )

    with pytest.raises(
        GuidelineImpactError,
        match="guideline_adoption_event_unlink_shape_invalid",
    ):
        replace(mutation.event, impact_digest="f" * 64)
    with pytest.raises(
        GuidelineImpactError,
        match="guideline_unlink_mutation_payload_invalid",
    ):
        replace(
            mutation,
            event=replace(
                mutation.event,
                from_revision_id="guideline-1-revision-99",
                from_semantic_version="9.9.9",
                from_revision_digest="9" * 64,
            ),
        )
    event_tampering = (
        {"binding_digest_before": "8" * 64},
        {"binding_head_digest_before": "7" * 64},
        {"binding_head_digest_after": "6" * 64},
        {"policy_set_digest_before": "5" * 64},
        {"policy_set_digest_after": "4" * 64},
        {"removed_rule_ids": ("rule-99",)},
    )
    for changes in event_tampering:
        with pytest.raises(
            GuidelineImpactError,
            match="guideline_unlink_mutation_payload_invalid",
        ):
            replace(
                mutation,
                event=replace(mutation.event, **changes),
            )


def _retirement(
    revision: GuidelineRevision,
    *,
    status: GuidelineLifecycleStatus = GuidelineLifecycleStatus.RETIRED,
) -> GuidelineRetirement:
    return GuidelineRetirement(
        retirement_id="retirement-1",
        guideline_id=revision.guideline_id,
        status=status,
        retired_revision_id=revision.revision_id,
        retired_revision_number=revision.revision_number,
        retired_semantic_version=revision.semantic_version,
        retired_revision_digest=revision.content_digest,
        retired_head_revision=revision.revision_number,
        reason="Policy is no longer applicable.",
        retired_by="agent-1",
        retired_at=NOW + timedelta(minutes=30),
        superseded_by_guideline_id=(
            "guideline-successor"
            if status is GuidelineLifecycleStatus.SUPERSEDED
            else None
        ),
    )


def test_retirement_planner_seals_one_board_tombstone_and_activity() -> None:
    current_revision = _revision(1, rules=(_rule(), _rule("rule-2")))
    current_binding = _binding(current_revision, priority=10)
    other_revision = _revision(1, guideline_id="guideline-2")
    other_binding = _binding(
        other_revision,
        binding_id="binding-2",
        priority=1,
    )
    retirement = _retirement(current_revision)
    arguments = {
        "retirement": retirement,
        "current_binding": current_binding,
        "current_revision": current_revision,
        "active_bindings": (current_binding, other_binding),
        "active_revisions": (current_revision, other_revision),
        "actor_type": "agent",
        "request_digest": "a" * 64,
    }

    mutation = plan_guideline_retirement_impact(**arguments)
    replay = plan_guideline_retirement_impact(**arguments)

    assert replay == mutation
    assert mutation.event.event_type == GUIDELINE_RETIREMENT_EVENT_TYPE
    assert mutation.event.operation == "retire"
    assert mutation.event.retirement_id == retirement.retirement_id
    assert mutation.event.binding_id == current_binding.binding_id
    assert mutation.event.removed_rule_ids == ("rule-1", "rule-2")
    assert mutation.activity_action == GUIDELINE_RETIREMENT_ACTIVITY_ACTION
    assert mutation.event.binding_head_digest_before == (
        policy_binding_head_digest_v1((other_binding, current_binding))
    )
    assert mutation.event.binding_head_digest_after == (
        policy_binding_head_digest_v1((other_binding,))
    )
    assert mutation.event.policy_set_digest_before == policy_set_digest_v1(
        (other_binding, current_binding),
        (other_revision, current_revision),
    )
    assert mutation.event.policy_set_digest_after == policy_set_digest_v1(
        (other_binding,),
        (other_revision,),
    )
    payload = mutation.event.payload()
    assert payload["policy_set_digest"] == payload["policy_set_digest_after"]
    assert payload["request_digest"] == "a" * 64


def test_retirement_impact_preserves_a_board_historical_revision_pin() -> None:
    pinned_revision = _revision(1)
    terminal_head = _revision(2)
    binding = _binding(pinned_revision)

    mutation = plan_guideline_retirement_impact(
        retirement=_retirement(terminal_head),
        current_binding=binding,
        current_revision=pinned_revision,
        active_bindings=(binding,),
        active_revisions=(pinned_revision,),
        actor_type="agent",
        request_digest="c" * 64,
    )

    assert mutation.retirement.retired_revision_id == (terminal_head.revision_id)
    assert mutation.event.revision_id == pinned_revision.revision_id
    assert mutation.event.revision_number == pinned_revision.revision_number
    assert mutation.event.revision_digest == pinned_revision.content_digest


def test_retirement_mutation_rejects_lineage_tampering() -> None:
    revision = _revision(1)
    binding = _binding(revision)
    mutation = plan_guideline_retirement_impact(
        retirement=_retirement(revision),
        current_binding=binding,
        current_revision=revision,
        active_bindings=(binding,),
        active_revisions=(revision,),
        actor_type="agent",
        request_digest="b" * 64,
    )

    with pytest.raises(
        GuidelineImpactError,
        match="guideline_retirement_impact_payload_invalid",
    ):
        replace(mutation, activity_action="guideline_deleted")
    event_tampering = (
        {"binding_digest_before": "8" * 64},
        {"binding_head_digest_before": "7" * 64},
        {"binding_head_digest_after": "6" * 64},
        {"policy_set_digest_before": "5" * 64},
        {"policy_set_digest_after": "4" * 64},
        {"removed_rule_ids": ("rule-99",)},
        {"revision_id": "guideline-1-revision-99"},
    )
    for changes in event_tampering:
        with pytest.raises(
            GuidelineImpactError,
            match="guideline_retirement_impact_payload_invalid",
        ):
            replace(
                mutation,
                event=replace(mutation.event, **changes),
            )
