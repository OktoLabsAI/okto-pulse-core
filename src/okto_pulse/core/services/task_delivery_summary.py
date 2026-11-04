"""Pure presentation policy shared by projection workers and authorized readers."""

from pydantic import ValidationError

from okto_pulse.core.models.delivery_readiness import DeliveryReadinessContext
from okto_pulse.core.models.delivery_summary import TaskRequirementSummary
from okto_pulse.core.models.task_delivery_summary import TaskDeliveryAction, TaskDeliverySummary, TaskDeliveryUpdate


_REASONS = {
    "updating": "Updating delivery status. Please wait for confirmation.",
    "unavailable": "Delivery status could not be loaded. Try again.",
    "assessment_incomplete": "There is not enough information to confirm delivery yet.",
    "optional": "This task is optional. Delivery has not been confirmed.",
    "waived": "This task has been waived. Delivery has not been confirmed.",
    "verification_failed": "Verification found a problem that needs to be fixed.",
    "historical_done": "The previous completion is in the history. Current delivery needs confirmation.",
    "implementation_pending": "Work or implementation evidence is still pending.",
    "not_started": "This task has not started yet.",
    "paused": "This task is on hold with work still pending.",
    "cancelled": "This task was cancelled. Its history is preserved, but delivery is not confirmed.",
    "verification_pending": "Verification is still pending for this delivery.",
    "completion_pending": "Some conditions must still be met to complete this delivery.",
    "completion_not_evaluated": "Evidence is recorded. Completion conditions still need confirmation.",
    "delivered": "Delivery is complete and confirmed by current information.",
    "ready": "Everything is ready to complete. Delivery has not been closed yet.",
}


def build_task_delivery_summary(context: DeliveryReadinessContext, *,
        requirement: TaskRequirementSummary | None, population_known: bool,
        evaluation_complete: bool) -> TaskDeliverySummary:
    """Never derive approval from percentages, an empty list, or stored Done.

    This actor-neutral payload has no next action. The authorized read projects
    that field after rechecking both freshness and the actual principal.
    """
    scope = context.scope
    if scope.card_id is None or scope.card_version is None:
        raise ValueError("task_delivery_card_scope_required")
    status = context.lifecycle.stored_status
    known = population_known and context.scope_complete
    evaluated = evaluation_complete and context.evaluation_complete
    state, reason = "updating_or_unknown", "assessment_incomplete"
    pending = None
    if known and evaluated and requirement is not None and requirement.current:
        pending = context.summary.unresolved_units
        if requirement.decision in {"optional", "waived"}:
            state, reason, pending = "optional_or_waived", requirement.decision, 0
        elif context.implementation.status == "unknown" or context.verification.status == "unknown":
            pending = None
        elif context.verification.counts_by_state.failed or context.verification.counts_by_state.conflict:
            state = reason = "verification_failed"
        elif status == "done" and not (
            context.proof_complete is True and context.delivery_gate.status == "allowed"
            and context.lifecycle.qualification_is_current is True
            and context.implementation.unresolved == context.verification.unresolved == 0
        ):
            state = reason = "historical_done"
        elif context.implementation.unresolved:
            state = "implementation_pending"
            reason = {"not_started": "not_started", "on_hold": "paused", "cancelled": "cancelled"}.get(
                status, "implementation_pending")
        elif context.verification.unresolved:
            state = reason = "verification_pending"
        elif status == "cancelled":
            state, reason, pending = "completion_pending", "cancelled", None
        elif context.proof_complete is not True or context.delivery_gate.status != "allowed":
            state = reason = "completion_pending"
        elif status == "done":
            state = reason = "delivered"
            pending = 0
        elif context.lifecycle.check_status != "allowed":
            state = "completion_pending"
            reason = ("completion_pending" if context.lifecycle.check_status == "blocked"
                      else "completion_not_evaluated")
            pending = context.lifecycle.other_blocker_count
        else:
            state = reason = "ready"
            pending = 0
    return TaskDeliverySummary(board_id=scope.board_id, spec_id=scope.spec_id, card_id=scope.card_id,
        spec_edition=scope.spec_edition, card_version=scope.card_version, snapshot_id=context.snapshot_id,
        population_known=known, evaluation_complete=evaluated, state=state, reason_code=reason,
        reason=_REASONS[reason], pending_count=pending, historical_status=status)


def present_task_delivery_summary(payload: dict | None, *, board_id: str, spec_id: str,
        card_id: str, spec_edition: int, card_version: int, status: str, freshness: str,
        actor_kind: str, allowed_operations: frozenset[str], last_update: dict | None = None) -> TaskDeliverySummary:
    """Apply current read authority without evaluating proof or writing state.

    A stale, missing or old-provider payload is explicitly unknown. Even an
    immutable stored payload cannot lend a previous caller's action authority.
    """
    summary = None
    if payload is not None:
        try:
            summary = TaskDeliverySummary.model_validate(payload)
        except ValidationError:
            pass
    aligned = bool(summary and (summary.board_id, summary.spec_id, summary.card_id,
        summary.spec_edition, summary.card_version, summary.historical_status) == (
            board_id, spec_id, card_id, spec_edition, card_version, status))
    readable = {"spec.entity.read", "card.entity.read", "code_traceability.evidence.read"} <= allowed_operations
    if freshness != "current" or not aligned or not readable:
        reason = ("updating" if freshness in {"updating", "pending_initial"} else "unavailable")
        summary = TaskDeliverySummary(board_id=board_id, spec_id=spec_id, card_id=card_id,
            spec_edition=spec_edition, card_version=card_version, snapshot_id=None,
            population_known=False, evaluation_complete=False, state="updating_or_unknown",
            reason_code=reason, reason=_REASONS[reason], pending_count=None, historical_status=status)
    action = None
    if readable:
        if summary.state == "updating_or_unknown":
            action = TaskDeliveryAction(kind="retry", label="Try again", owner="system")
        elif summary.state != "delivered":
            action = TaskDeliveryAction(kind="open_details", label="View details", owner="technical_owner")
            if actor_kind in {"human", "user"} and status == "in_progress" and "card.conclusion.write" in allowed_operations:
                if summary.state == "ready" and "card.move.in_progress_to_done" in allowed_operations:
                    action = TaskDeliveryAction(kind="conclude", label="Complete delivery", owner="user")
                elif summary.state == "completion_pending" and "card.move.in_progress_to_validation" in allowed_operations:
                    action = TaskDeliveryAction(kind="submit_report", label="Submit delivery", owner="user")
                elif summary.state == "implementation_pending":
                    action = TaskDeliveryAction(kind="record_progress", label="Record progress", owner="user")
    note = TaskDeliveryUpdate.model_validate(last_update) if last_update and readable else None
    return summary.model_copy(update={"next_action": action, "last_update": note})
