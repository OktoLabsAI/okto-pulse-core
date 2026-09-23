"""Authenticated shared REST/MCP delivery evidence entry point."""

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
    decide_authorization,
    resolve_actor_permissions,
)
from okto_pulse.core.application.use_cases.base import PermissionDeniedError, commit
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.models.delivery_evidence import (
    CardDeliveryEvidenceBatchCommand,
    CardDeliveryEvidenceWriteCommand,
    DeliveryEvidenceCommand,
    DeliveryEvidenceQuery,
    DeliveryEvidenceReadQuery,
)
from okto_pulse.core.models.delivery_report import CardDeliveryReportCommand, DeliveryReportRejected
from okto_pulse.core.application.use_cases.mutation_permissions import transition_permission_requirement


class GetDeliveryEvidenceUseCase:
    async def execute(
        self, command: DeliveryEvidenceQuery, *, actor, uow: PulseUnitOfWork
    ):
        await require_authorization(
            actor,
            PermissionRequirement("code_traceability.evidence.read"),
            uow=uow,
            board_id=command.board_id,
        )
        if isinstance(command, DeliveryEvidenceReadQuery) and command.card_id is not None:
            if command.view == "resume":
                result = await uow.services.delivery_evidence.card_resume(command, actor_id=actor.actor_id)
                permissions = await resolve_actor_permissions(actor, uow, command.board_id)
                result["actions"] = {
                    "read_progress_history": True,
                    "record_progress": result.pop("progress_state_eligible", False) and decide_authorization(
                        actor, PermissionRequirement("card.conclusion.write"), permissions=permissions).allowed,
                    "final_transitions": "not_evaluated",
                    "mutation_reauthorization_required": True,
                }
                return result
            return await uow.services.delivery_evidence.progress_history(command, actor_id=actor.actor_id)
        return await uow.services.delivery_evidence.projection(
            command.board_id, command.spec_id
        )


class RecordDeliveryEvidenceUseCase:
    """Spec-scoped exceptions only; historical proof remains readable."""

    async def execute(
        self, command: DeliveryEvidenceCommand, *, actor, uow: PulseUnitOfWork
    ):
        operation = {
            "implementation": "code_traceability.target.execution_submit",
            "test": "spec.tests.execute",
            "waiver": "code_traceability.waiver.create",
            "revoke": "code_traceability.waiver.clear",
        }[command.kind]
        await require_authorization(
            actor, PermissionRequirement(operation), uow=uow, board_id=command.board_id
        )
        command.require_exception_kind()
        if command.kind in {"waiver", "revoke"} and actor.actor_kind not in {
            "human",
            "user",
        }:
            raise PermissionDeniedError(
                "delivery_evidence_human_authorization_required"
            )
        result = await uow.services.delivery_evidence.record(
            command, actor_id=actor.actor_id, actor_kind=actor.actor_kind
        )
        await commit(uow)
        return result


class RecordCardDeliveryEvidenceUseCase:
    """Card-scoped recording surface (spec 793c43d0 / FR-7).

    The task owns its implementation/test bindings; the command carries the
    card CAS fence (expected_card_version) and the spec edition. Waivers are
    deliberately absent — they stay on the legacy spec-rollup surface and are
    human-only (BR-3). Revoke keeps the human-only rule.
    """

    def __init__(self, execution_use_case=None):
        self._execution_use_case = execution_use_case

    async def execute(
        self, command: CardDeliveryEvidenceWriteCommand | CardDeliveryReportCommand, *, actor, uow: PulseUnitOfWork
    ):
        if isinstance(command, CardDeliveryReportCommand):
            return await self.submit_report(command, actor=actor, uow=uow)
        options = await self.authorize_in_transaction(command, actor=actor, uow=uow)
        result = await uow.services.delivery_evidence.record_card(
            command, actor_id=actor.actor_id, actor_kind=actor.actor_kind, **options
        )
        await commit(uow)
        return result

    async def authorize_in_transaction(self, command, *, actor, uow: PulseUnitOfWork):
        """Authorize every constituent before any write or replay payload read."""
        operations = {
            "progress": "card.conclusion.write",
            "implementation": "code_traceability.target.execution_submit",
            "test": "spec.tests.execute",
            "revoke": "code_traceability.waiver.clear",
        }
        entries = command.entries if isinstance(command, CardDeliveryEvidenceBatchCommand) else (command,)
        # All subactions are authorized before the first mutation or replay read.
        for operation in sorted({operations[entry.kind] for entry in entries}):
            await require_authorization(
                actor, PermissionRequirement(operation), uow=uow, board_id=command.board_id
            )
        if any(entry.kind == "revoke" for entry in entries) and actor.actor_kind not in {"human", "user"}:
            raise PermissionDeniedError(
                "delivery_evidence_human_authorization_required"
            )
        store = uow.services.delivery_evidence
        record_card = getattr(store, "record_card", None)
        if record_card is None:
            raise PermissionDeniedError(
                "card_delivery_evidence_adapter_unavailable"
            )
        options = {}
        if any(entry.execution_submission is not None for entry in entries):
            if self._execution_use_case is None:
                raise PermissionDeniedError("delivery_execution_submitter_unavailable")
            await self._execution_use_case.authorize_in_transaction(
                board_id=command.board_id, actor=actor, uow=uow
            )

            async def submit_execution(submission):
                admitted = await self._execution_use_case.execute_in_transaction(
                    submission, actor=actor, uow=uow
                )
                return admitted.record.id

            options["execution_submitter"] = submit_execution
        return options

    async def submit_report(self, command, *, actor, uow: PulseUnitOfWork):
        batch = command.batch_command()
        options = await self.authorize_in_transaction(batch, actor=actor, uow=uow)
        await require_authorization(actor, transition_permission_requirement(
            "card", command.expected_card_status, command.report.status, legacy_operation="cards:move"
        ), uow=uow, board_id=command.board_id)

        async def submit(selection):
            try:
                moved = await uow.services.cards.move_card(command.card_id, actor.actor_id,
                    command.report.model_copy(update={"delivery_selection": selection}), actor.actor_name)
            except ValueError as exc:
                raise DeliveryReportRejected(exc) from exc
            if moved is None:
                raise ValueError("delivery_report_card_unavailable")

        try:
            result = await uow.services.delivery_evidence.record_card_report(
                command, actor_id=actor.actor_id, actor_kind=actor.actor_kind,
                report_submitter=submit, **options,
            )
            await commit(uow)
            return result
        except BaseException:
            await uow.rollback()
            raise
