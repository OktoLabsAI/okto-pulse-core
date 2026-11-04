"""Pure presentation policy shared by projection workers and authorized readers."""

from pydantic import ValidationError

from okto_pulse.core.models.delivery_readiness import DeliveryReadinessContext
from okto_pulse.core.models.delivery_summary import TaskRequirementSummary
from okto_pulse.core.models.task_delivery_summary import TaskDeliveryAction, TaskDeliverySummary, TaskDeliveryUpdate


_REASONS = {
    "updating": "Atualizando a situação da entrega. Aguarde a confirmação atual.",
    "unavailable": "Não foi possível consultar a entrega. Tente novamente.",
    "assessment_incomplete": "Ainda não há informação suficiente para confirmar a entrega.",
    "optional": "Esta tarefa é opcional. Isso não significa que foi entregue.",
    "waived": "Esta tarefa foi dispensada. Isso não significa que foi entregue.",
    "verification_failed": "A verificação encontrou um problema que precisa ser corrigido.",
    "historical_done": "A conclusão anterior está no histórico; a entrega atual precisa de confirmação.",
    "implementation_pending": "Ainda há trabalho ou comprovação de implementação pendente.",
    "not_started": "Esta tarefa ainda não foi iniciada.",
    "paused": "Esta tarefa está pausada e ainda possui trabalho pendente.",
    "cancelled": "Esta tarefa foi cancelada. O histórico foi preservado, sem confirmar entrega.",
    "verification_pending": "A implementação está registrada; falta concluir a verificação.",
    "completion_pending": "A entrega ainda depende de condições para ser concluída.",
    "completion_not_evaluated": "As provas estão registradas; falta confirmar as condições de conclusão.",
    "delivered": "Entrega concluída e confirmada com as informações atuais.",
    "ready": "Tudo está pronto para concluir. A entrega ainda não foi encerrada.",
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
            action = TaskDeliveryAction(kind="retry", label="Tentar novamente", owner="system")
        elif summary.state != "delivered":
            action = TaskDeliveryAction(kind="open_details", label="Ver detalhes", owner="technical_owner")
            if actor_kind in {"human", "user"} and status == "in_progress" and "card.conclusion.write" in allowed_operations:
                if summary.state == "ready" and "card.move.in_progress_to_done" in allowed_operations:
                    action = TaskDeliveryAction(kind="conclude", label="Concluir entrega", owner="user")
                elif summary.state == "completion_pending" and "card.move.in_progress_to_validation" in allowed_operations:
                    action = TaskDeliveryAction(kind="submit_report", label="Enviar entrega", owner="user")
                elif summary.state == "implementation_pending":
                    action = TaskDeliveryAction(kind="record_progress", label="Registrar progresso", owner="user")
    note = TaskDeliveryUpdate.model_validate(last_update) if last_update and readable else None
    return summary.model_copy(update={"next_action": action, "last_update": note})
