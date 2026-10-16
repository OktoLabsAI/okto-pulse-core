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
        if transition.action is AssessmentLifecycleAction.ADMIT_VALIDATION:
            # Admission freezes the edition's governance scope through the
            # persistence port in the caller-owned UoW. Existing human-result
            # heads remain unchanged; no new result exists until an external
            # evaluator submits it.
            return AssessmentLifecyclePlan(
                transition=transition,
                head_strategy=AssessmentHeadStrategy.PRESERVE,
                projection_action=AssessmentProjectionAction.REBUILD,
                kg_action=AssessmentKgAction.RECONCILE,
                head_rebuilds=(),
                preserve_immutable_history=True,
                event_and_outbox_same_uow=True,
                clear_checklist_execution_head=False,
            )
        if not recover:
            return AssessmentLifecyclePlan(
                transition=transition,
                head_strategy=AssessmentHeadStrategy.PRESERVE,
                projection_action=AssessmentProjectionAction.HIDE,
                kg_action=AssessmentKgAction.TOMBSTONE,
                head_rebuilds=(),
                preserve_immutable_history=True,
                event_and_outbox_same_uow=True,
                clear_checklist_execution_head=False,
            )

        if transition.action is AssessmentLifecycleAction.REOPEN:
            # Opening a new lifecycle edition has no current human result. Clear
            # every mutable head association while preserving all immutable rows.
            heads_by_kind = {
                item.assessment_kind: item for item in resolved_heads
            }
            rebuilds = tuple(
                AssessmentHeadRebuild(
                    assessment_kind=kind,
                    expected_revision=head.revision,
                    previous_receipt_id=head.receipt_id,
                    selected_receipt_id=None,
                    selected_state=None,
                    resulting_revision=head.revision + 1,
                )
                for kind, head in sorted(
                    heads_by_kind.items(),
                    key=lambda item: item[0].value,
                )
            )
            return AssessmentLifecyclePlan(
                transition=transition,
                head_strategy=AssessmentHeadStrategy.RECOMPUTE,
                projection_action=AssessmentProjectionAction.REBUILD,
                kg_action=AssessmentKgAction.RECONCILE,
                head_rebuilds=rebuilds,
                preserve_immutable_history=True,
                event_and_outbox_same_uow=True,
                clear_checklist_execution_head=(
                    transition.after.subject.subject_type.value == "spec"
                ),
            )

        # Restore only the current edition's head. Technical versions and input
        # digests remain audit evidence, not human-result currentness selectors.
        heads_by_kind = {
            item.assessment_kind: item for item in resolved_heads
        }
        kinds = sorted(
            {item.assessment_kind for item in resolved_receipts} | set(heads_by_kind),
            key=lambda item: item.value,
        )
        rebuilds: list[AssessmentHeadRebuild] = []
        for kind in kinds:
            current_receipts = [
                receipt
                for receipt in resolved_receipts
                if receipt.assessment_kind is kind
                and receipt.subject.subject_edition
                == transition.after.subject.subject_edition
            ]
            selected = max(
                current_receipts,
                key=lambda item: (item.created_at, item.receipt_id),
                default=None,
            )
            previous_head = heads_by_kind.get(kind)
            previous_receipt_id = (
                previous_head.receipt_id if previous_head is not None else None
            )
            selected_receipt_id = selected.receipt_id if selected is not None else None
            expected_revision = previous_head.revision if previous_head is not None else 0
            rebuilds.append(
                AssessmentHeadRebuild(
                    assessment_kind=kind,
                    expected_revision=expected_revision,
                    previous_receipt_id=previous_receipt_id,
                    selected_receipt_id=selected_receipt_id,
                    selected_state=(
                        AssessmentReceiptState.CURRENT if selected is not None else None
                    ),
                    resulting_revision=expected_revision + int(
                        previous_receipt_id != selected_receipt_id
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
            clear_checklist_execution_head=False,
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
