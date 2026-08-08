from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.quality_assessment import (
    AssessmentKind,
    AssessmentSubjectRef,
    AssessmentSubjectType,
)
from okto_pulse.core.domain.quality_assessment_lifecycle import (
    ASSESSMENT_BOARD_PURGE_ORDER,
    ASSESSMENT_SUBJECT_PURGE_ORDER,
    AssessmentBoardErasureCompletion,
    AssessmentHeadStrategy,
    AssessmentKgAction,
    AssessmentLifecycleAction,
    AssessmentLifecycleContractError,
    AssessmentLifecycleCurrentInput,
    AssessmentLifecycleHead,
    AssessmentLifecycleReceipt,
    AssessmentLifecycleSubjectSnapshot,
    AssessmentLifecycleTransition,
    AssessmentProjectionAction,
    AssessmentPurgePostcondition,
    AssessmentPurgeResidual,
    AssessmentPurgeResource,
    AssessmentPurgeScope,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.services.quality_assessment_lifecycle import (
    AssessmentLifecycleExecutionError,
    QualityAssessmentLifecycleService,
)

NOW = datetime(2026, 7, 27, 19, 0, tzinfo=timezone.utc)
INPUT_DIGEST = canonical_sha256("current-input")
OTHER_DIGEST = canonical_sha256("stale-input")


def _snapshot(
    *,
    version: int = 4,
    status: str = "done",
    archived: bool = False,
    include_input: bool = True,
) -> AssessmentLifecycleSubjectSnapshot:
    return AssessmentLifecycleSubjectSnapshot(
        subject=AssessmentSubjectRef(
            board_id="b1",
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="s1",
            subject_version=version,
        ),
        status=status,
        archived=archived,
        current_inputs=(
            (
                AssessmentLifecycleCurrentInput(
                    assessment_kind=AssessmentKind.SPEC_VALIDATION,
                    input_digest=INPUT_DIGEST,
                ),
            )
            if include_input
            else ()
        ),
    )


def _head(
    receipt_id: str = "receipt-old",
    *,
    revision: int = 3,
) -> AssessmentLifecycleHead:
    return AssessmentLifecycleHead(
        assessment_kind=AssessmentKind.SPEC_VALIDATION,
        receipt_id=receipt_id,
        revision=revision,
    )


def _receipt(
    receipt_id: str,
    *,
    version: int = 4,
    digest: str = INPUT_DIGEST,
    created_at: datetime = NOW,
) -> AssessmentLifecycleReceipt:
    return AssessmentLifecycleReceipt(
        receipt_id=receipt_id,
        subject=AssessmentSubjectRef(
            board_id="b1",
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="s1",
            subject_version=version,
        ),
        assessment_kind=AssessmentKind.SPEC_VALIDATION,
        input_digest=digest,
        created_at=created_at,
    )


def _transition(
    action: AssessmentLifecycleAction,
    *,
    before: AssessmentLifecycleSubjectSnapshot,
    after: AssessmentLifecycleSubjectSnapshot,
    idempotency_key: str = "lifecycle-op-1",
) -> AssessmentLifecycleTransition:
    return AssessmentLifecycleTransition(
        action=action,
        before=before,
        after=after,
        idempotency_key=idempotency_key,
        actor_id="agent-1",
        occurred_at=NOW,
    )


@pytest.mark.parametrize(
    ("transition", "action"),
    [
        (
            _transition(
                AssessmentLifecycleAction.ARCHIVE,
                before=_snapshot(archived=False),
                after=_snapshot(archived=True),
            ),
            AssessmentLifecycleAction.ARCHIVE,
        ),
        (
            _transition(
                AssessmentLifecycleAction.CANCEL,
                before=_snapshot(status="approved"),
                after=_snapshot(status="cancelled"),
            ),
            AssessmentLifecycleAction.CANCEL,
        ),
    ],
)
def test_archive_and_cancel_preserve_history_and_hide_current_projection(
    transition: AssessmentLifecycleTransition,
    action: AssessmentLifecycleAction,
) -> None:
    plan = QualityAssessmentLifecycleService().prepare_transition(
        transition,
        heads=(_head(),),
        receipts=(_receipt("receipt-old"),),
    )

    assert plan.transition.action is action
    assert plan.head_strategy is AssessmentHeadStrategy.PRESERVE
    assert plan.projection_action is AssessmentProjectionAction.HIDE
    assert plan.kg_action is AssessmentKgAction.TOMBSTONE
    assert plan.head_rebuilds == ()
    assert plan.preserve_immutable_history is True
    assert plan.event_and_outbox_same_uow is True


def test_restore_recomputes_head_from_latest_exact_current_receipt() -> None:
    transition = _transition(
        AssessmentLifecycleAction.RESTORE,
        before=_snapshot(archived=True),
        after=_snapshot(archived=False),
    )
    receipts = (
        _receipt(
            "receipt-stale-input",
            digest=OTHER_DIGEST,
            created_at=NOW + timedelta(minutes=3),
        ),
        _receipt(
            "receipt-old-version",
            version=3,
            created_at=NOW + timedelta(minutes=4),
        ),
        _receipt("receipt-current-a", created_at=NOW),
        _receipt(
            "receipt-current-b",
            created_at=NOW + timedelta(minutes=1),
        ),
    )

    plan = QualityAssessmentLifecycleService().prepare_transition(
        transition,
        heads=(_head("orphan-head", revision=7),),
        receipts=receipts,
    )

    assert plan.head_strategy is AssessmentHeadStrategy.RECOMPUTE
    assert plan.projection_action is AssessmentProjectionAction.REBUILD
    assert plan.kg_action is AssessmentKgAction.RECONCILE
    assert len(plan.head_rebuilds) == 1
    rebuild = plan.head_rebuilds[0]
    assert rebuild.previous_receipt_id == "orphan-head"
    assert rebuild.selected_receipt_id == "receipt-current-b"
    assert rebuild.selected_state.value == "current"
    assert rebuild.expected_revision == 7
    assert rebuild.resulting_revision == 8
    assert rebuild.stale_transition_required is False
    assert rebuild.stale_transition_key is None


def test_restore_keeps_already_correct_head_without_revision_bump() -> None:
    transition = _transition(
        AssessmentLifecycleAction.RESTORE,
        before=_snapshot(archived=True),
        after=_snapshot(archived=False),
    )
    plan = QualityAssessmentLifecycleService().prepare_transition(
        transition,
        heads=(_head("receipt-current", revision=8),),
        receipts=(_receipt("receipt-current"),),
    )

    rebuild = plan.head_rebuilds[0]
    assert rebuild.selected_receipt_id == "receipt-current"
    assert rebuild.resulting_revision == 8
    assert rebuild.stale_transition_required is False


def test_reopen_preserves_valid_stale_head_with_replay_stable_transition() -> None:
    transition = _transition(
        AssessmentLifecycleAction.REOPEN,
        before=_snapshot(version=4, status="done"),
        after=_snapshot(version=5, status="draft"),
    )
    service = QualityAssessmentLifecycleService()
    first = service.prepare_transition(
        transition,
        heads=(_head("receipt-v4", revision=4),),
        receipts=(_receipt("receipt-v4", version=4),),
    )

    first_rebuild = first.head_rebuilds[0]
    assert first_rebuild.selected_receipt_id == "receipt-v4"
    assert first_rebuild.selected_state.value == "stale"
    assert first_rebuild.resulting_revision == 4
    assert first_rebuild.stale_transition_required is True
    assert first_rebuild.stale_transition_key is not None

    # Replaying the same lifecycle operation yields the same transition key;
    # the adapter's unique idempotency fence records "became stale" once.
    retry = service.prepare_transition(
        transition,
        heads=(_head("receipt-v4", revision=4),),
        receipts=(_receipt("receipt-v4", version=4),),
    )
    retry_rebuild = retry.head_rebuilds[0]
    assert retry_rebuild.selected_receipt_id == "receipt-v4"
    assert retry_rebuild.resulting_revision == 4
    assert retry_rebuild.stale_transition_required is True
    assert (
        retry_rebuild.stale_transition_key
        == first_rebuild.stale_transition_key
    )

    distinct_operation = service.prepare_transition(
        _transition(
            AssessmentLifecycleAction.REOPEN,
            before=_snapshot(version=4, status="done"),
            after=_snapshot(version=5, status="draft"),
            idempotency_key="lifecycle-op-2",
        ),
        heads=(_head("receipt-v4", revision=4),),
        receipts=(_receipt("receipt-v4", version=4),),
    )
    assert (
        distinct_operation.head_rebuilds[0].stale_transition_key
        != first_rebuild.stale_transition_key
    )


def test_orphan_head_is_invalidated_but_not_misreported_as_stale() -> None:
    transition = _transition(
        AssessmentLifecycleAction.RESTORE,
        before=_snapshot(archived=True),
        after=_snapshot(archived=False),
    )
    plan = QualityAssessmentLifecycleService().prepare_transition(
        transition,
        heads=(_head("missing-receipt", revision=9),),
        receipts=(),
    )

    rebuild = plan.head_rebuilds[0]
    assert rebuild.previous_receipt_id == "missing-receipt"
    assert rebuild.selected_receipt_id is None
    assert rebuild.selected_state is None
    assert rebuild.resulting_revision == 10
    assert rebuild.stale_transition_required is False
    assert rebuild.stale_transition_key is None


def test_invalid_transition_and_cross_subject_receipt_fail_closed() -> None:
    with pytest.raises(
        AssessmentLifecycleContractError,
        match="assessment_lifecycle_transition_invalid",
    ):
        _transition(
            AssessmentLifecycleAction.REOPEN,
            before=_snapshot(version=4, status="done"),
            after=_snapshot(version=4, status="draft"),
        )

    transition = _transition(
        AssessmentLifecycleAction.RESTORE,
        before=_snapshot(archived=True),
        after=_snapshot(archived=False),
    )
    foreign = AssessmentLifecycleReceipt(
        receipt_id="foreign",
        subject=AssessmentSubjectRef(
            board_id="other",
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="s1",
            subject_version=4,
        ),
        assessment_kind=AssessmentKind.SPEC_VALIDATION,
        input_digest=INPUT_DIGEST,
        created_at=NOW,
    )
    with pytest.raises(
        AssessmentLifecycleContractError,
        match="assessment_lifecycle_receipt_scope_mismatch",
    ):
        QualityAssessmentLifecycleService().prepare_transition(
            transition,
            receipts=(foreign,),
        )


def test_subject_purge_is_ordered_idempotent_and_preserves_epoch() -> None:
    service = QualityAssessmentLifecycleService()
    plan = service.prepare_subject_purge(
        board_id="b1",
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="s1",
    )

    assert plan.target.scope is AssessmentPurgeScope.SUBJECT
    assert plan.deletion_order == ASSESSMENT_SUBJECT_PURGE_ORDER
    assert plan.idempotent is True
    assert plan.board_erasure_permit_id is None
    assert AssessmentPurgeResource.QUALITY_FINDING_QA_LINKS in plan.deletion_order
    assert (
        plan.deletion_order.index(AssessmentPurgeResource.QUALITY_HEADS)
        < plan.deletion_order.index(AssessmentPurgeResource.QUALITY_RECEIPTS)
    )
    assert (
        AssessmentPurgeResource.LEGACY_IMPORT_CANDIDATES
        not in plan.deletion_order
    )


def test_board_purge_requires_permit_and_includes_epoch_last() -> None:
    service = QualityAssessmentLifecycleService()
    plan = service.prepare_board_purge(
        board_id="b1",
        board_erasure_permit_id="permit-1",
    )

    assert plan.target.scope is AssessmentPurgeScope.BOARD
    assert plan.deletion_order == ASSESSMENT_BOARD_PURGE_ORDER
    assert plan.deletion_order[-3:] == (
        AssessmentPurgeResource.LEGACY_IMPORT_COMPLETIONS,
        AssessmentPurgeResource.LEGACY_IMPORT_CANDIDATES,
        AssessmentPurgeResource.LEGACY_IMPORT_RUNS,
    )
    assert plan.deletion_order[-5:] == (
        AssessmentPurgeResource.LEGACY_IMPORT_RESOLUTIONS,
        AssessmentPurgeResource.LEGACY_IMPORT_CHECKPOINTS,
        AssessmentPurgeResource.LEGACY_IMPORT_COMPLETIONS,
        AssessmentPurgeResource.LEGACY_IMPORT_CANDIDATES,
        AssessmentPurgeResource.LEGACY_IMPORT_RUNS,
    )
    assert plan.board_erasure_permit_id == "permit-1"

    with pytest.raises(
        AssessmentLifecycleContractError,
        match="assessment_board_erasure_permit_required",
    ):
        service.prepare_board_purge(
            board_id="b1",
            board_erasure_permit_id=" ",
        )

    with pytest.raises(TypeError, match="board_erasure_permit_released"):
        AssessmentPurgePostcondition(
            target=plan.target,
            residuals=tuple(
                AssessmentPurgeResidual(resource=resource, count=0)
                for resource in plan.deletion_order
            ),
            zero_orphans=True,
            projections_reconciled=True,
            outbox_reconciled=True,
            epoch_consistency_preserved=True,
            # Inner purge evidence cannot fabricate the outer permit result.
            board_erasure_permit_released=False,
            verified_at=NOW,
        )


def _postcondition(plan, *, residual_override=None, **flags):
    residuals = tuple(
        AssessmentPurgeResidual(
            resource=resource,
            count=(
                residual_override[1]
                if residual_override is not None
                and resource is residual_override[0]
                else 0
            ),
        )
        for resource in plan.deletion_order
    )
    values = {
        "zero_orphans": True,
        "projections_reconciled": True,
        "outbox_reconciled": True,
        "epoch_consistency_preserved": True,
    }
    values.update(flags)
    return AssessmentPurgePostcondition(
        target=plan.target,
        residuals=residuals,
        verified_at=NOW,
        **values,
    )


def test_outer_board_erasure_alone_attests_permit_release() -> None:
    service = QualityAssessmentLifecycleService()
    plan = service.prepare_board_purge(
        board_id="b1",
        board_erasure_permit_id="permit-1",
    )
    inner = _postcondition(plan)

    # The inner proof is valid while the permit is still active and has no
    # field through which the adapter could claim release.
    service.validate_purge_postcondition(plan=plan, postcondition=inner)
    assert not hasattr(inner, "board_erasure_permit_released")

    incomplete = AssessmentBoardErasureCompletion(
        target=plan.target,
        quality_purge_postcondition=inner,
        board_erasure_permit_id="permit-1",
        all_board_purges_completed=False,
        permit_released=True,
        verified_at=NOW,
    )
    with pytest.raises(
        AssessmentLifecycleExecutionError,
        match="assessment_board_erasure_purges_incomplete",
    ):
        service.validate_board_erasure_completion(
            plan=plan,
            inner_postcondition=inner,
            completion=incomplete,
        )

    unreleased = AssessmentBoardErasureCompletion(
        target=plan.target,
        quality_purge_postcondition=inner,
        board_erasure_permit_id="permit-1",
        all_board_purges_completed=True,
        permit_released=False,
        verified_at=NOW,
    )
    with pytest.raises(
        AssessmentLifecycleExecutionError,
        match="assessment_board_erasure_permit_release_failed",
    ):
        service.validate_board_erasure_completion(
            plan=plan,
            inner_postcondition=inner,
            completion=unreleased,
        )

    complete = AssessmentBoardErasureCompletion(
        target=plan.target,
        quality_purge_postcondition=inner,
        board_erasure_permit_id="permit-1",
        all_board_purges_completed=True,
        permit_released=True,
        verified_at=NOW,
    )
    service.validate_board_erasure_completion(
        plan=plan,
        inner_postcondition=inner,
        completion=complete,
    )


def test_purge_postcondition_is_complete_zero_orphan_and_retry_safe() -> None:
    service = QualityAssessmentLifecycleService()
    plan = service.prepare_subject_purge(
        board_id="b1",
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="s1",
    )
    postcondition = _postcondition(plan)

    service.validate_purge_postcondition(
        plan=plan,
        postcondition=postcondition,
    )
    # A second zero-residual execution is a valid idempotent retry.
    service.validate_purge_postcondition(
        plan=plan,
        postcondition=postcondition,
    )


@pytest.mark.parametrize(
    ("postcondition_factory", "error"),
    [
        (
            lambda plan: _postcondition(
                plan,
                residual_override=(AssessmentPurgeResource.QUALITY_HEADS, 1),
            ),
            "assessment_purge_residual_rows",
        ),
        (
            lambda plan: _postcondition(plan, zero_orphans=False),
            "assessment_purge_orphans_detected",
        ),
        (
            lambda plan: _postcondition(
                plan,
                projections_reconciled=False,
            ),
            "assessment_purge_projection_reconciliation_failed",
        ),
        (
            lambda plan: _postcondition(plan, outbox_reconciled=False),
            "assessment_purge_outbox_reconciliation_failed",
        ),
        (
            lambda plan: _postcondition(
                plan,
                epoch_consistency_preserved=False,
            ),
            "assessment_purge_epoch_consistency_failed",
        ),
    ],
    ids=(
        "residual-row",
        "orphan",
        "projection",
        "outbox",
        "epoch",
    ),
)
def test_purge_postcondition_failures_are_closed(
    postcondition_factory,
    error: str,
) -> None:
    service = QualityAssessmentLifecycleService()
    plan = service.prepare_subject_purge(
        board_id="b1",
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="s1",
    )

    with pytest.raises(AssessmentLifecycleExecutionError, match=error):
        service.validate_purge_postcondition(
            plan=plan,
            postcondition=postcondition_factory(plan),
        )


def test_missing_purge_residual_evidence_fails_closed() -> None:
    service = QualityAssessmentLifecycleService()
    plan = service.prepare_subject_purge(
        board_id="b1",
        subject_type=AssessmentSubjectType.SPEC,
        subject_id="s1",
    )
    postcondition = AssessmentPurgePostcondition(
        target=plan.target,
        residuals=tuple(
            AssessmentPurgeResidual(resource=resource, count=0)
            for resource in plan.deletion_order[:-1]
        ),
        zero_orphans=True,
        projections_reconciled=True,
        outbox_reconciled=True,
        epoch_consistency_preserved=True,
        verified_at=NOW,
    )

    with pytest.raises(
        AssessmentLifecycleExecutionError,
        match="assessment_purge_postcondition_incomplete",
    ):
        service.validate_purge_postcondition(
            plan=plan,
            postcondition=postcondition,
        )
