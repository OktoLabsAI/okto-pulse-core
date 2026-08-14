"""Transport-free selective Knowledge propagation v2 orchestration.

All creation paths perform target-independent parent/source/auth preflight,
then stage the deterministic target and its propagation ledger in the same
caller-owned UoW.  Existing-target mutations likewise commit the target CAS,
append-only records, and result_v2 receipt atomically.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import (
    load_accessible_board,
    load_accessible_card,
)
from okto_pulse.core.domain.knowledge_selection import (
    KnowledgeTargetType,
)
from okto_pulse.core.models.knowledge_propagation import (
    KnowledgeAssignmentDropRequest,
    KnowledgeAssignmentRefreshRequest,
    KnowledgeAssignmentReplaceRequest,
    KnowledgePropagationEnvelopeV2,
)
from okto_pulse.core.ports.application_persistence import (
    ApplicationRecordConflictError,
)
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgeMutationReceipt,
    KnowledgeParentKey,
    KnowledgeParentType,
    KnowledgeTargetKey,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgeCreationPreflightCommand,
    KnowledgeMutationCommand,
    KnowledgeMutationPreparation,
    KnowledgeMutationResultV2,
    KnowledgePropagationServiceError,
    KnowledgeRefreshByKnowledgeIdsCommand,
    deterministic_knowledge_target_id,
)
from okto_pulse.core.services.card_operational_freeze import (
    require_card_operational_mutation_allowed,
)


_CARD_WRITE_SHARE_PERMISSIONS = {"editor", "admin"}


def _activity_actor_type(actor: ActorContext) -> str:
    """Map transport source to the persisted activity actor taxonomy."""

    if actor.source == "mcp":
        return "agent"
    if actor.source == "rest":
        return "user"
    return actor.source


def _semantic_creation_hash(
    *,
    operation: str,
    parent: KnowledgeParentKey,
    payload: Mapping[str, object],
) -> str:
    encoded = json.dumps(
        {
            "contract_version": 2,
            "operation": operation,
            "parent": parent.to_dict(),
            "payload": dict(payload),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _model_payload(value: Any, *, exclude: set[str] | None = None) -> dict[str, object]:
    dump = getattr(value, "model_dump", None)
    if dump is None:
        raise TypeError("knowledge_propagation_creation_payload_invalid")
    payload = dump(mode="json", exclude=exclude or set())
    if not isinstance(payload, dict):
        raise TypeError("knowledge_propagation_creation_payload_invalid")
    return payload


def _creation_preflight_command(
    *,
    parent: KnowledgeParentKey,
    target_type: KnowledgeTargetType,
    envelope: KnowledgePropagationEnvelopeV2,
    actor_id: str,
    semantic_creation_hash: str,
    creation_result: Mapping[str, object],
) -> KnowledgeCreationPreflightCommand:
    return KnowledgeCreationPreflightCommand(
        parent=parent,
        target_type=target_type,
        selection=envelope.to_selection(),
        actor_id=actor_id,
        idempotency_key=envelope.idempotency_key,
        expected_revision=envelope.expected_revision,
        justification=envelope.justification,
        relevance_links=envelope.to_relevance_links(),
        semantic_creation_hash=semantic_creation_hash,
        creation_result=creation_result,
    )


class KnowledgeMutationUseCaseResult:
    __slots__ = ("receipt", "result_v2")

    def __init__(
        self,
        receipt: KnowledgeMutationReceipt,
        result_v2: KnowledgeMutationResultV2,
    ) -> None:
        self.receipt = receipt
        self.result_v2 = result_v2


class KnowledgeCreationRaceError(KnowledgePropagationServiceError):
    """Edition UoWs raise this when deterministic target uniqueness loses."""

    retryable = True

    def __init__(self, target: KnowledgeTargetKey) -> None:
        if not isinstance(target, KnowledgeTargetKey):
            raise TypeError("knowledge_creation_race_target_invalid")
        self.target = target
        super().__init__(
            "knowledge_creation_race",
            (
                "another request created the deterministic target; retry in a "
                "fresh unit of work to recover the durable replay"
            ),
            details={"target": target.to_dict()},
        )


def _mutation_result(
    services: Any,
    receipt: KnowledgeMutationReceipt,
) -> KnowledgeMutationUseCaseResult:
    result_v2 = services.knowledge_propagation.result_from_receipt(receipt)
    return KnowledgeMutationUseCaseResult(receipt, result_v2)


async def _verify_replayed_creation_target(
    *,
    uow: PulseUnitOfWork,
    parent: KnowledgeParentKey,
    receipt: KnowledgeMutationReceipt,
) -> None:
    target = receipt.target
    if target.target_type is KnowledgeTargetType.SPEC:
        entity = await uow.services.specs.get_spec(target.target_id)
        parent_matches = bool(
            entity
            and entity.board_id == parent.board_id
            and getattr(entity, "refinement_id", None) == parent.parent_id
        )
    else:
        entity = await uow.services.cards.get_card(target.target_id)
        parent_matches = bool(
            entity
            and entity.board_id == parent.board_id
            and getattr(entity, "spec_id", None) == parent.parent_id
        )
    if not parent_matches:
        raise KnowledgePropagationServiceError(
            "knowledge_creation_replay_target_mismatch",
            "the durable creation receipt has no matching deterministic target",
            details={
                "parent": parent.to_dict(),
                "target": target.to_dict(),
            },
        )


async def _require_target_absent(
    *,
    uow: PulseUnitOfWork,
    target: KnowledgeTargetKey,
) -> None:
    existing = (
        await uow.services.specs.get_spec(target.target_id)
        if target.target_type is KnowledgeTargetType.SPEC
        else await uow.services.cards.get_card(target.target_id)
    )
    if existing is not None:
        raise KnowledgePropagationServiceError(
            "knowledge_creation_target_collision",
            "the deterministic target exists without a matching durable receipt",
            details={"target": target.to_dict()},
        )


def _raise_creation_record_conflict(
    error: ApplicationRecordConflictError,
    target: KnowledgeTargetKey,
) -> None:
    if (
        error.entity != target.target_type.value
        or error.record_id != target.target_id
    ):
        raise error
    raise KnowledgeCreationRaceError(target) from error


async def _resolve_card_parent_spec_id(
    *,
    uow: PulseUnitOfWork,
    board_id: str,
    data: Any,
) -> str | None:
    """Resolve the authoritative parent before v2 preflight and hashing.

    Legacy bug creation derives its Spec from ``origin_task_id``.  V2 must do
    that before validating Knowledge IDs; otherwise it can validate against
    one Spec and persist the card under another.
    """

    requested_spec_id = getattr(data, "spec_id", None)
    card_type = getattr(data, "card_type", "normal") or "normal"
    if card_type != "bug":
        return requested_spec_id

    origin_task_id = getattr(data, "origin_task_id", None)
    if not origin_task_id:
        raise ValueError("origin_task_id is required for bug cards")
    origin_task = await uow.services.cards.get_card(origin_task_id)
    if origin_task is None or origin_task.board_id != board_id:
        raise ValueError("Origin task not found on this board")
    effective_spec_id = getattr(origin_task, "spec_id", None)
    if not effective_spec_id:
        raise ValueError(
            "Origin task has no linked spec — bug cards require a spec-linked task"
        )
    if requested_spec_id and requested_spec_id != effective_spec_id:
        raise KnowledgePropagationServiceError(
            "knowledge_propagation_parent_conflict",
            "bug card spec_id conflicts with the origin task's authoritative Spec",
            details={
                "requested_spec_id": requested_spec_id,
                "origin_task_id": origin_task_id,
                "origin_spec_id": effective_spec_id,
            },
        )
    data.spec_id = effective_spec_id
    return effective_spec_id


class DeriveSpecKnowledgeV2Command:
    __slots__ = (
        "refinement_id",
        "envelope",
        "mockup_ids",
        "architecture_design_ids",
        "architecture_propagation_mode",
    )

    def __init__(
        self,
        refinement_id: str,
        envelope: KnowledgePropagationEnvelopeV2,
        *,
        mockup_ids: list[str] | None = None,
        architecture_design_ids: list[str] | None = None,
        architecture_propagation_mode: str = "copy",
    ) -> None:
        self.refinement_id = refinement_id
        self.envelope = envelope
        self.mockup_ids = mockup_ids
        self.architecture_design_ids = architecture_design_ids
        self.architecture_propagation_mode = architecture_propagation_mode


class DeriveSpecKnowledgeV2UseCase:
    async def execute(
        self,
        command: DeriveSpecKnowledgeV2Command,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> KnowledgeMutationUseCaseResult:
        refinement = await uow.services.refinements.get_refinement(
            command.refinement_id
        )
        if refinement is None or (
            actor.board_id is not None and refinement.board_id != actor.board_id
        ):
            raise EntityNotFoundError("refinement", command.refinement_id)
        if actor.source != "mcp":
            board = await load_accessible_board(
                uow,
                refinement.board_id,
                actor,
                allowed_share_permissions=_CARD_WRITE_SHARE_PERMISSIONS,
            )
            if board is None:
                raise EntityNotFoundError("refinement", command.refinement_id)
        parent = KnowledgeParentKey(
            board_id=refinement.board_id,
            parent_type=KnowledgeParentType.REFINEMENT,
            parent_id=command.refinement_id,
        )
        semantic_hash = _semantic_creation_hash(
            operation="derive_spec",
            parent=parent,
            payload={
                "mockup_ids": command.mockup_ids,
                "architecture_design_ids": command.architecture_design_ids,
                "architecture_propagation_mode": (
                    command.architecture_propagation_mode
                ),
            },
        )
        target_id = deterministic_knowledge_target_id(
            parent,
            KnowledgeTargetType.SPEC,
            command.envelope.idempotency_key,
        )
        preflight = await uow.services.knowledge_propagation.preflight_creation(
            _creation_preflight_command(
                parent=parent,
                target_type=KnowledgeTargetType.SPEC,
                envelope=command.envelope,
                actor_id=actor.actor_id,
                semantic_creation_hash=semantic_hash,
                creation_result={"spec_id": target_id},
            )
        )
        if isinstance(preflight, KnowledgeMutationReceipt):
            await _verify_replayed_creation_target(
                uow=uow,
                parent=parent,
                receipt=preflight,
            )
            await commit(uow)  # persist the replay-attempt observation
            return _mutation_result(uow.services, preflight)
        if not isinstance(preflight, KnowledgeMutationPreparation):
            raise TypeError("knowledge_propagation_preflight_result_invalid")

        await _require_target_absent(uow=uow, target=preflight.command.target)
        try:
            spec = await uow.services.refinements.derive_spec(
                command.refinement_id,
                actor.actor_id,
                # Authorization was completed above for both transports. Avoid a
                # second owner-only service check that would reject a REST editor
                # who legitimately reached this application boundary via a share.
                skip_ownership_check=True,
                mockup_ids=command.mockup_ids,
                kb_ids=None,
                architecture_design_ids=command.architecture_design_ids,
                architecture_propagation_mode=(
                    command.architecture_propagation_mode
                ),
                target_id=preflight.command.target.target_id,
                knowledge_propagation_v2=True,
            )
        except ApplicationRecordConflictError as error:
            _raise_creation_record_conflict(error, preflight.command.target)
        if spec is None:
            raise EntityNotFoundError("refinement", command.refinement_id)
        # Surface a deterministic-id uniqueness race before staging the ledger.
        await uow.synchronize(
            conflict_error=KnowledgeCreationRaceError(
                preflight.command.target,
            )
        )
        from okto_pulse.core.application.use_cases.research_decision_ledger import (
            bind_research_decisions_to_spec,
        )

        await bind_research_decisions_to_spec(
            refinement=refinement,
            spec=spec,
            uow=uow,
        )
        receipt = await uow.services.knowledge_propagation.mutate(preflight)
        await commit(uow)
        return _mutation_result(uow.services, receipt)


class CreateCardKnowledgeV2Command:
    __slots__ = ("board_id", "data", "skip_ownership_check", "activity_details")

    def __init__(
        self,
        board_id: str,
        data: Any,
        *,
        skip_ownership_check: bool = False,
        activity_details: dict[str, object] | None = None,
    ) -> None:
        self.board_id = board_id
        self.data = data
        self.skip_ownership_check = skip_ownership_check
        self.activity_details = activity_details


class CreateCardKnowledgeV2UseCase:
    async def execute(
        self,
        command: CreateCardKnowledgeV2Command,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> KnowledgeMutationUseCaseResult:
        envelope = getattr(command.data, "knowledge_propagation", None)
        if not isinstance(envelope, KnowledgePropagationEnvelopeV2):
            raise ValueError("knowledge_propagation_envelope_required")
        spec_id = await _resolve_card_parent_spec_id(
            uow=uow,
            board_id=command.board_id,
            data=command.data,
        )
        spec = await uow.services.specs.get_spec(spec_id) if spec_id else None
        if (
            spec is None
            or (
                actor.board_id is not None
                and actor.board_id != command.board_id
            )
            or spec.board_id != command.board_id
        ):
            raise ValueError(f"Spec '{spec_id}' not found")
        parent = KnowledgeParentKey(
            board_id=command.board_id,
            parent_type=KnowledgeParentType.SPEC,
            parent_id=spec.id,
        )
        semantic_hash = _semantic_creation_hash(
            operation="create_card",
            parent=parent,
            payload=_model_payload(
                command.data,
                exclude={"knowledge_propagation"},
            ),
        )
        target_id = deterministic_knowledge_target_id(
            parent,
            KnowledgeTargetType.CARD,
            envelope.idempotency_key,
        )
        card_payload = _model_payload(
            command.data,
            exclude={"knowledge_propagation"},
        )
        creation_result = {
            "card": {
                "id": target_id,
                "board_id": command.board_id,
                "spec_id": spec.id,
                "title": card_payload.get("title"),
                "description": card_payload.get("description"),
                "details": card_payload.get("details"),
                "status": card_payload.get("status"),
                "priority": card_payload.get("priority"),
                "card_type": card_payload.get("card_type"),
            }
        }
        preflight = await uow.services.knowledge_propagation.preflight_creation(
            _creation_preflight_command(
                parent=parent,
                target_type=KnowledgeTargetType.CARD,
                envelope=envelope,
                actor_id=actor.actor_id,
                semantic_creation_hash=semantic_hash,
                creation_result=creation_result,
            )
        )
        if isinstance(preflight, KnowledgeMutationReceipt):
            await _verify_replayed_creation_target(
                uow=uow,
                parent=parent,
                receipt=preflight,
            )
            await commit(uow)
            return _mutation_result(uow.services, preflight)
        if not isinstance(preflight, KnowledgeMutationPreparation):
            raise TypeError("knowledge_propagation_preflight_result_invalid")

        await _require_target_absent(uow=uow, target=preflight.command.target)
        try:
            card = await uow.services.cards.create_card(
                command.board_id,
                actor.actor_id,
                command.data,
                skip_ownership_check=command.skip_ownership_check,
                target_id=preflight.command.target.target_id,
                knowledge_propagation_v2=True,
                actor_type=_activity_actor_type(actor),
                actor_name=actor.actor_name,
                activity_details=command.activity_details,
            )
        except ApplicationRecordConflictError as error:
            _raise_creation_record_conflict(error, preflight.command.target)
        if card is None:
            raise EntityNotFoundError("board", command.board_id)
        await uow.synchronize(
            conflict_error=KnowledgeCreationRaceError(
                preflight.command.target,
            )
        )
        receipt = await uow.services.knowledge_propagation.mutate(preflight)
        await commit(uow)
        return _mutation_result(uow.services, receipt)


async def _load_card_for_v2_write(
    uow: PulseUnitOfWork,
    card_id: str,
    actor: ActorContext,
) -> Any:
    card = await load_accessible_card(
        uow,
        card_id,
        actor,
        expected_board_id=actor.board_id,
        allowed_share_permissions=_CARD_WRITE_SHARE_PERMISSIONS,
    )
    if card is None:
        raise EntityNotFoundError("card", card_id)
    require_card_operational_mutation_allowed(
        card,
        operation="mutate_card_knowledge_assignments",
    )
    return card


class ReplaceCardKnowledgeAssignmentsCommand:
    __slots__ = ("card_id", "request")

    def __init__(
        self,
        card_id: str,
        request: KnowledgeAssignmentReplaceRequest,
    ) -> None:
        self.card_id = card_id
        self.request = request


class ReplaceCardKnowledgeAssignmentsUseCase:
    async def execute(
        self,
        command: ReplaceCardKnowledgeAssignmentsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> KnowledgeMutationUseCaseResult:
        card = await _load_card_for_v2_write(uow, command.card_id, actor)
        if not card.spec_id:
            raise KnowledgePropagationServiceError(
                "knowledge_relevance_spec_missing",
                "the card has no valid linked spec",
            )
        envelope = command.request.to_envelope()
        receipt = await uow.services.knowledge_propagation.mutate(
            KnowledgeMutationCommand(
                target=KnowledgeTargetKey(
                    board_id=card.board_id,
                    target_type=KnowledgeTargetType.CARD,
                    target_id=card.id,
                ),
                selection=envelope.to_selection(),
                actor_id=actor.actor_id,
                expected_revision=command.request.expected_revision,
                idempotency_key=command.request.idempotency_key,
                justification=command.request.justification,
                relevance_links=envelope.to_relevance_links(),
                parent=KnowledgeParentKey(
                    board_id=card.board_id,
                    parent_type=KnowledgeParentType.SPEC,
                    parent_id=card.spec_id,
                ),
            )
        )
        await commit(uow)
        return _mutation_result(uow.services, receipt)


class DropCardKnowledgeAssignmentsCommand:
    __slots__ = ("card_id", "request")

    def __init__(
        self,
        card_id: str,
        request: KnowledgeAssignmentDropRequest,
    ) -> None:
        self.card_id = card_id
        self.request = request


class DropCardKnowledgeAssignmentsUseCase:
    async def execute(
        self,
        command: DropCardKnowledgeAssignmentsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> KnowledgeMutationUseCaseResult:
        card = await _load_card_for_v2_write(uow, command.card_id, actor)
        envelope = command.request.to_envelope()
        receipt = await uow.services.knowledge_propagation.mutate(
            KnowledgeMutationCommand(
                target=KnowledgeTargetKey(
                    board_id=card.board_id,
                    target_type=KnowledgeTargetType.CARD,
                    target_id=card.id,
                ),
                selection=envelope.to_selection(),
                actor_id=actor.actor_id,
                expected_revision=command.request.expected_revision,
                idempotency_key=command.request.idempotency_key,
                justification=command.request.justification,
            )
        )
        await commit(uow)
        return _mutation_result(uow.services, receipt)


class RefreshCardKnowledgeAssignmentsCommand:
    __slots__ = ("card_id", "request")

    def __init__(
        self,
        card_id: str,
        request: KnowledgeAssignmentRefreshRequest,
    ) -> None:
        self.card_id = card_id
        self.request = request


class RefreshCardKnowledgeAssignmentsUseCase:
    async def execute(
        self,
        command: RefreshCardKnowledgeAssignmentsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> KnowledgeMutationUseCaseResult:
        card = await _load_card_for_v2_write(uow, command.card_id, actor)
        receipt = await uow.services.knowledge_propagation.refresh_by_knowledge_ids(
            KnowledgeRefreshByKnowledgeIdsCommand(
                target=KnowledgeTargetKey(
                    board_id=card.board_id,
                    target_type=KnowledgeTargetType.CARD,
                    target_id=card.id,
                ),
                knowledge_ids=tuple(command.request.knowledge_ids),
                actor_id=actor.actor_id,
                expected_revision=command.request.expected_revision,
                idempotency_key=command.request.idempotency_key,
            )
        )
        await commit(uow)
        return _mutation_result(uow.services, receipt)


class GetCardKnowledgePropagationCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class GetCardKnowledgePropagationResult:
    __slots__ = ("read_result",)

    def __init__(self, read_result: Any) -> None:
        self.read_result = read_result


class GetCardKnowledgePropagationUseCase:
    async def execute(
        self,
        command: GetCardKnowledgePropagationCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetCardKnowledgePropagationResult:
        card = await load_accessible_card(
            uow,
            command.card_id,
            actor,
            expected_board_id=actor.board_id,
        )
        if card is None:
            raise EntityNotFoundError("card", command.card_id)
        result = await uow.services.knowledge_propagation.read(
            KnowledgeTargetKey(
                board_id=card.board_id,
                target_type=KnowledgeTargetType.CARD,
                target_id=card.id,
            )
        )
        return GetCardKnowledgePropagationResult(result)


__all__ = [
    "CreateCardKnowledgeV2Command",
    "CreateCardKnowledgeV2UseCase",
    "DeriveSpecKnowledgeV2Command",
    "DeriveSpecKnowledgeV2UseCase",
    "DropCardKnowledgeAssignmentsCommand",
    "DropCardKnowledgeAssignmentsUseCase",
    "GetCardKnowledgePropagationCommand",
    "GetCardKnowledgePropagationResult",
    "GetCardKnowledgePropagationUseCase",
    "KnowledgeMutationUseCaseResult",
    "KnowledgeCreationRaceError",
    "RefreshCardKnowledgeAssignmentsCommand",
    "RefreshCardKnowledgeAssignmentsUseCase",
    "ReplaceCardKnowledgeAssignmentsCommand",
    "ReplaceCardKnowledgeAssignmentsUseCase",
]
