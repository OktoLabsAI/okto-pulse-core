"""Authenticated shared REST/MCP delivery evidence entry point."""

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import PermissionDeniedError, commit
from okto_pulse.core.models.delivery_evidence import (
    DeliveryEvidenceCommand,
    DeliveryEvidenceQuery,
)


class GetDeliveryEvidenceUseCase:
    async def execute(self, command: DeliveryEvidenceQuery, *, actor, uow):
        await require_authorization(
            actor,
            PermissionRequirement("code_traceability.evidence.read"),
            uow=uow,
            board_id=command.board_id,
        )
        return await uow.services.delivery_evidence.projection(
            command.board_id, command.spec_id
        )


class RecordDeliveryEvidenceUseCase:
    async def execute(self, command: DeliveryEvidenceCommand, *, actor, uow):
        operation = {
            "implementation": "code_traceability.target.execution_submit",
            "test": "spec.tests.execute",
            "waiver": "code_traceability.waiver.create",
            "revoke": "code_traceability.waiver.clear",
        }[command.kind]
        await require_authorization(
            actor, PermissionRequirement(operation), uow=uow, board_id=command.board_id
        )
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
