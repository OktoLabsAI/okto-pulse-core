"""Fail-closed policy for quality lifecycle reconciliation and purge."""

from __future__ import annotations

from collections.abc import Iterable

from okto_pulse.core.domain.quality_assessment import AssessmentReceiptState
from okto_pulse.core.domain.quality_assessment_lifecycle import (
    ASSESSMENT_BOARD_PURGE_ORDER,
    ASSESSMENT_SUBJECT_PURGE_ORDER,
    AssessmentBoardErasureCompletion,
    AssessmentHeadRebuild,
    AssessmentHeadStrategy,
    AssessmentKgAction,
    AssessmentLifecycleAction,
    AssessmentLifecycleContractError,
    AssessmentLifecycleHead,
    AssessmentLifecyclePlan,
    AssessmentLifecycleReceipt,
    AssessmentLifecycleTransition,
    AssessmentProjectionAction,
    AssessmentPurgePlan,
    AssessmentPurgePostcondition,
    AssessmentPurgeResource,
    AssessmentPurgeScope,
    AssessmentPurgeTarget,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


class AssessmentLifecycleExecutionError(RuntimeError):
    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


class QualityAssessmentLifecycleService:
    """Create adapter-neutral transition and destructive-purge plans."""

    def prepare_transition(
        self,
        transition: AssessmentLifecycleTransition,
        *,
        heads: Iterable[AssessmentLifecycleHead] = (),
        receipts: Iterable[AssessmentLifecycleReceipt] = (),
    ) -> AssessmentLifecyclePlan:
        if not isinstance(transition, AssessmentLifecycleTransition):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_transition_invalid"
            )
        resolved_heads = tuple(heads)
        resolved_receipts = tuple(receipts)
        if any(
            not isinstance(item, AssessmentLifecycleHead)
            for item in resolved_heads
        ):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_head_invalid"
            )
        if any(
            not isinstance(item, AssessmentLifecycleReceipt)
            for item in resolved_receipts
        ):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_receipt_invalid"
            )
        head_kinds = [item.assessment_kind for item in resolved_heads]
        if len(set(head_kinds)) != len(head_kinds):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_head_duplicate"
            )
        receipt_ids = [item.receipt_id for item in resolved_receipts]
        if len(set(receipt_ids)) != len(receipt_ids):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_receipt_duplicate"
            )
        identity = transition.after.identity
        if any(
            (
                item.subject.board_id,
                item.subject.subject_type,
                item.subject.subject_id,
            )
            != identity
            for item in resolved_receipts
        ):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_receipt_scope_mismatch"
            )

        recover = transition.action in {
            AssessmentLifecycleAction.RESTORE,
            AssessmentLifecycleAction.REOPEN,
        }
        if not recover:
            return AssessmentLifecyclePlan(
                transition=transition,
                head_strategy=AssessmentHeadStrategy.PRESERVE,
                projection_action=AssessmentProjectionAction.HIDE,
                kg_action=AssessmentKgAction.TOMBSTONE,
                head_rebuilds=(),
                preserve_immutable_history=True,
                event_and_outbox_same_uow=True,
            )

        inputs = {
            item.assessment_kind: item.input_digest
            for item in transition.after.current_inputs
        }
        heads_by_kind = {
            item.assessment_kind: item for item in resolved_heads
        }
        kinds = sorted(
            set(inputs) | set(heads_by_kind),
            key=lambda item: item.value,
        )
        rebuilds: list[AssessmentHeadRebuild] = []
        for kind in kinds:
            current_input = inputs.get(kind)
            kind_receipts = [
                receipt
                for receipt in resolved_receipts
                if receipt.assessment_kind is kind
            ]
            current_receipts = [
                receipt
                for receipt in kind_receipts
                if (
                    receipt.subject.subject_version
                    == transition.after.subject.subject_version
                    and current_input is not None
                    and receipt.input_digest == current_input
                )
            ]
            selection_pool = current_receipts or kind_receipts
            selected = (
                max(
                    selection_pool,
                    key=lambda item: (item.created_at, item.receipt_id),
                )
                if selection_pool
                else None
            )
            selected_state = (
                (
                    AssessmentReceiptState.CURRENT
                    if selected in current_receipts
                    else AssessmentReceiptState.STALE
                )
                if selected is not None
                else None
            )
            previous_head = heads_by_kind.get(kind)
            previous_receipt_id = (
                previous_head.receipt_id
                if previous_head is not None
                else None
            )
            selected_receipt_id = (
                selected.receipt_id if selected is not None else None
            )
            expected_revision = (
                previous_head.revision if previous_head is not None else 0
            )
            changed = previous_receipt_id != selected_receipt_id
            previous_receipt = next(
                (
                    receipt
                    for receipt in kind_receipts
                    if receipt.receipt_id == previous_receipt_id
                ),
                None,
            )
            before_inputs = {
                item.assessment_kind: item.input_digest
                for item in transition.before.current_inputs
            }
            previous_was_current = (
                previous_receipt is not None
                and previous_receipt.subject.subject_version
                == transition.before.subject.subject_version
                and previous_receipt.input_digest
                == before_inputs.get(kind)
            )
            stale_transition_required = (
                previous_was_current
                and selected_state is AssessmentReceiptState.STALE
            )
            rebuilds.append(
                AssessmentHeadRebuild(
                    assessment_kind=kind,
                    expected_revision=expected_revision,
                    previous_receipt_id=previous_receipt_id,
                    selected_receipt_id=selected_receipt_id,
                    selected_state=selected_state,
                    resulting_revision=expected_revision + int(changed),
                    stale_transition_required=stale_transition_required,
                    stale_transition_key=(
                        canonical_sha256(
                            {
                                "transition_digest": (
                                    transition.transition_digest
                                ),
                                "assessment_kind": kind.value,
                                "receipt_id": previous_receipt_id,
                                "state": "stale",
                            }
                        )
                        if stale_transition_required
                        else None
                    ),
                )
            )
        return AssessmentLifecyclePlan(
            transition=transition,
            head_strategy=AssessmentHeadStrategy.RECOMPUTE,
            projection_action=AssessmentProjectionAction.REBUILD,
            kg_action=AssessmentKgAction.RECONCILE,
            head_rebuilds=tuple(rebuilds),
            preserve_immutable_history=True,
            event_and_outbox_same_uow=True,
        )

    def prepare_subject_purge(
        self,
        *,
        board_id: str,
        subject_type,
        subject_id: str,
    ) -> AssessmentPurgePlan:
        target = AssessmentPurgeTarget(
            board_id=board_id,
            scope=AssessmentPurgeScope.SUBJECT,
            subject_type=subject_type,
            subject_id=subject_id,
        )
        return AssessmentPurgePlan(
            target=target,
            deletion_order=ASSESSMENT_SUBJECT_PURGE_ORDER,
            board_erasure_permit_id=None,
            idempotent=True,
            event_and_outbox_same_uow=True,
        )

    def prepare_board_purge(
        self,
        *,
        board_id: str,
        board_erasure_permit_id: str,
    ) -> AssessmentPurgePlan:
        target = AssessmentPurgeTarget(
            board_id=board_id,
            scope=AssessmentPurgeScope.BOARD,
        )
        return AssessmentPurgePlan(
            target=target,
            deletion_order=ASSESSMENT_BOARD_PURGE_ORDER,
            board_erasure_permit_id=board_erasure_permit_id,
            idempotent=True,
            event_and_outbox_same_uow=True,
        )

    def validate_purge_postcondition(
        self,
        *,
        plan: AssessmentPurgePlan,
        postcondition: AssessmentPurgePostcondition,
    ) -> None:
        if not isinstance(plan, AssessmentPurgePlan) or not isinstance(
            postcondition,
            AssessmentPurgePostcondition,
        ):
            raise AssessmentLifecycleContractError(
                "assessment_purge_validation_input_invalid"
            )
        if postcondition.target != plan.target:
            raise AssessmentLifecycleExecutionError(
                "assessment_purge_postcondition_target_mismatch"
            )
        residual_by_resource = {
            item.resource: item.count for item in postcondition.residuals
        }
        expected_resources = set(plan.deletion_order)
        if set(residual_by_resource) != expected_resources:
            raise AssessmentLifecycleExecutionError(
                "assessment_purge_postcondition_incomplete"
            )
        nonzero = {
            resource: count
            for resource, count in residual_by_resource.items()
            if count != 0
        }
        if nonzero:
            raise AssessmentLifecycleExecutionError(
                "assessment_purge_residual_rows"
            )
        if not postcondition.zero_orphans:
            raise AssessmentLifecycleExecutionError(
                "assessment_purge_orphans_detected"
            )
        if not postcondition.projections_reconciled:
            raise AssessmentLifecycleExecutionError(
                "assessment_purge_projection_reconciliation_failed"
            )
        if not postcondition.outbox_reconciled:
            raise AssessmentLifecycleExecutionError(
                "assessment_purge_outbox_reconciliation_failed"
            )
        if not postcondition.epoch_consistency_preserved:
            raise AssessmentLifecycleExecutionError(
                "assessment_purge_epoch_consistency_failed"
            )

    def validate_board_erasure_completion(
        self,
        *,
        plan: AssessmentPurgePlan,
        inner_postcondition: AssessmentPurgePostcondition,
        completion: AssessmentBoardErasureCompletion,
    ) -> None:
        """Validate the outer proof after every purge and permit release.

        ``validate_purge_postcondition`` deliberately cannot validate permit
        release because its adapter executes inside that permit's lifetime.
        This separate method is the only Core boundary that accepts the outer
        orchestrator's completion evidence.
        """

        if (
            not isinstance(plan, AssessmentPurgePlan)
            or plan.target.scope is not AssessmentPurgeScope.BOARD
        ):
            raise AssessmentLifecycleContractError(
                "assessment_board_erasure_plan_required"
            )
        if not isinstance(completion, AssessmentBoardErasureCompletion):
            raise AssessmentLifecycleContractError(
                "assessment_board_erasure_completion_invalid"
            )
        self.validate_purge_postcondition(
            plan=plan,
            postcondition=inner_postcondition,
        )
        if completion.target != plan.target:
            raise AssessmentLifecycleExecutionError(
                "assessment_board_erasure_completion_target_mismatch"
            )
        if completion.quality_purge_postcondition != inner_postcondition:
            raise AssessmentLifecycleExecutionError(
                "assessment_board_erasure_inner_postcondition_mismatch"
            )
        if completion.board_erasure_permit_id != plan.board_erasure_permit_id:
            raise AssessmentLifecycleExecutionError(
                "assessment_board_erasure_permit_mismatch"
            )
        if not completion.all_board_purges_completed:
            raise AssessmentLifecycleExecutionError(
                "assessment_board_erasure_purges_incomplete"
            )
        if not completion.permit_released:
            raise AssessmentLifecycleExecutionError(
                "assessment_board_erasure_permit_release_failed"
            )

    @staticmethod
    def purge_resources_for_scope(
        scope: AssessmentPurgeScope,
    ) -> tuple[AssessmentPurgeResource, ...]:
        if scope is AssessmentPurgeScope.SUBJECT:
            return ASSESSMENT_SUBJECT_PURGE_ORDER
        if scope is AssessmentPurgeScope.BOARD:
            return ASSESSMENT_BOARD_PURGE_ORDER
        raise AssessmentLifecycleContractError(
            "assessment_purge_scope_invalid"
        )
