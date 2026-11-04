"""One authorized human intent, one atomic application-owned commit."""

import logging

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_authorization
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, PermissionDeniedError, commit
from okto_pulse.core.application.use_cases.mutation_permissions import transition_permission_requirement
from okto_pulse.core.models.delivery_report import DeliveryReportRejected
from okto_pulse.core.models.human_task_submission import HumanTaskSubmissionCommand, HumanTaskSubmissionRequest, HumanSubmissionConflict
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

logger = logging.getLogger(__name__)


def _record_outcome(command, outcome):
    # Neither command text, key, actor claims nor exception messages belong in
    # operational logs. The transactional ledger retains authorized detail.
    logger.info("task.human_submission %s", outcome, extra={
        "submission_outcome": outcome, "submission_action": command.action,
        "board_id": command.board_id, "card_id": command.card_id,
    })


class SubmitHumanTaskUseCase:
    async def execute(self, command: HumanTaskSubmissionRequest, *, actor, uow: PulseUnitOfWork):
        # Source (REST/MCP) is not identity. An agent using REST is still an agent.
        if actor.actor_kind not in {"human", "user"}:
            _record_outcome(command, "rejected")
            raise PermissionDeniedError("human_submission_human_authorization_required")
        try:
            await uow.begin_write()
            for operation in ("card.entity.read", "spec.entity.read", "card.conclusion.write"):
                await require_authorization(actor, PermissionRequirement(operation),
                                            uow=uow, board_id=command.board_id)
            if not isinstance(command, HumanTaskSubmissionCommand):
                card = await uow.services.cards.get_card(command.card_id)
                if card is None or card.board_id != command.board_id or not card.spec_id or card.archived:
                    raise EntityNotFoundError("Card", command.card_id)
                command = HumanTaskSubmissionCommand.model_validate({
                    **command.model_dump(exclude_unset=True), "spec_id": card.spec_id})
            if command.action == "delivery_report":
                # Reauthorize before the adapter reads an idempotent result too.
                await require_authorization(actor, transition_permission_requirement(
                    "card", "in_progress", command.requested_status), uow=uow, board_id=command.board_id)

            async def submit_report(report):
                # The adapter resolves existing authoritative assessment, impact
                # and compatible proof. User text never supplies those facts.
                from okto_pulse.core.application.use_cases.learning_capture import authorize_learning_submission
                await authorize_learning_submission(report, actor=actor, uow=uow, board_id=command.board_id)
                try:
                    moved = await uow.services.cards.move_card(
                        command.card_id, actor.actor_id, report, actor.actor_name)
                except ValueError as exc:
                    raise DeliveryReportRejected(exc) from exc
                if moved is None:
                    raise ValueError("human_submission_card_unavailable")

            result = await uow.services.delivery_evidence.record_human_submission(
                command, actor_id=actor.actor_id, actor_kind=actor.actor_kind,
                report_submitter=submit_report if command.action == "delivery_report" else None)
            await commit(uow)
            _record_outcome(command, "replayed" if result.get("replayed") else "accepted")
            return result
        except BaseException as exc:
            await uow.rollback()
            _record_outcome(command, "rejected" if isinstance(exc, (
                PermissionDeniedError, EntityNotFoundError, HumanSubmissionConflict, DeliveryReportRejected
            )) else "failed")
            raise
