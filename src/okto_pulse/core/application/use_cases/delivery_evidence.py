"""Authenticated shared REST/MCP delivery evidence entry point."""

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import PermissionDeniedError, commit
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.models.delivery_evidence import (
    CardDeliveryEvidenceBatchCommand,
    CardDeliveryEvidenceWriteCommand,
    DeliveryEvidenceCommand,
    DeliveryEvidenceQuery,
)


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

    async def execute(
        self, command: CardDeliveryEvidenceWriteCommand, *, actor, uow: PulseUnitOfWork
    ):
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
        result = await record_card(
            command, actor_id=actor.actor_id, actor_kind=actor.actor_kind
        )
        await commit(uow)
        return result
