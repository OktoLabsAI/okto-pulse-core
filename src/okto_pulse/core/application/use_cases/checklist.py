"""Transport-neutral A3 checklist application use cases."""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.checklist import (
    SPECIFY_CHECKLIST_TEMPLATE_V1,
    ChecklistBinding,
    ChecklistCommitResult,
    ChecklistCurrentness,
    ChecklistExecutionStartResult,
    ChecklistExecutionStatus,
    ChecklistGateDecision,
    ChecklistItemResult,
    ChecklistMode,
    ChecklistPage,
    ChecklistPhase,
    ChecklistPreflight,
    ChecklistReceipt,
    ChecklistReceiptState,
    ChecklistReceiptView,
    ChecklistSpecSnapshot,
    ChecklistSubmission,
    ChecklistTargetType,
    ChecklistTemplate,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.domain.permissions import Permissions
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.ports.checklist import ChecklistListQuery
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.checklist import (
    ChecklistConflictError,
    ChecklistService,
)

_READ_PERMISSIONS = (Permissions.BOARD_READ, "spec.checklist.read")
_EXECUTE_PERMISSIONS = (Permissions.SPECS_UPDATE, "spec.checklist.execute")


async def _require_permissions(
    services: ApplicationServiceCatalog,
    actor_id: str,
    board_id: str,
    permissions: tuple[str, ...],
) -> None:
    from okto_pulse.core.services.permission_policy import check_permission

    permission_set = await services.resolve_user_permissions(actor_id, board_id)
    for permission in permissions:
        error = check_permission(permission_set, permission)
        if error:
            raise PermissionDeniedError(error)


async def _require_board(
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    actor: ActorContext,
    write: bool,
):
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions={"editor", "admin"} if write else None,
    )
    if board is None:
        raise EntityNotFoundError("board", board_id)
    return board


async def _require_spec(
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    spec_id: str,
):
    spec = await uow.services.specs.get_spec(spec_id)
    if spec is None or getattr(spec, "board_id", None) != board_id:
        raise EntityNotFoundError("spec", spec_id)
    return spec


async def _preflight(
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    spec_id: str,
) -> ChecklistPreflight:
    persistence = uow.services.checklists
    subject = await persistence.get_spec_snapshot(
        board_id=board_id,
        spec_id=spec_id,
    )
    if subject is None:
        raise EntityNotFoundError("spec", spec_id)
    if not isinstance(subject, ChecklistSpecSnapshot):
        raise RuntimeError("checklist_subject_port_invalid")
    if subject.spec_edition is None:
        binding = await persistence.get_binding(
            board_id=board_id,
            target_type=ChecklistTargetType.SPEC,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
        if binding is None:
            binding = ChecklistBinding.synthetic_off(board_id=board_id)
    else:
        binding = await persistence.get_validation_binding(
            board_id=board_id,
            spec_id=spec_id,
            spec_edition=subject.spec_edition,
            target_type=ChecklistTargetType.SPEC,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
        if not isinstance(binding, ChecklistBinding):
            raise RuntimeError("checklist_validation_binding_port_invalid")
    try:
        current = await persistence.get_current(
            board_id=board_id,
            spec_id=spec_id,
            phase=ChecklistPhase.SPEC_VALIDATION,
            spec_edition=subject.spec_edition,
        )
    except TypeError:
        current = await persistence.get_current(
            board_id=board_id,
            spec_id=spec_id,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
    if current is not None and current[0].spec_edition != subject.spec_edition:
        current = None
    return ChecklistPreflight(
        subject=subject,
        binding=binding,
        current_head_revision=0 if current is None else current[1].revision,
        current_head_receipt_id=None if current is None else current[1].receipt_id,
    )


@dataclass(frozen=True, slots=True)
class UpdateChecklistBindingCommand:
    board_id: str
    mode: ChecklistMode
    expected_revision: int
    target_type: ChecklistTargetType = ChecklistTargetType.SPEC
    phase: ChecklistPhase = ChecklistPhase.SPEC_VALIDATION


class UpdateChecklistBindingUseCase:
    async def execute(
        self,
        command: UpdateChecklistBindingCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ChecklistBinding:
        await _require_board(
            uow,
            board_id=command.board_id,
            actor=actor,
            write=True,
        )
        if actor.source != "rest":
            raise PermissionDeniedError("human_actor_required")
        if (
            command.target_type is not ChecklistTargetType.SPEC
            or command.phase is not ChecklistPhase.SPEC_VALIDATION
        ):
            raise ChecklistConflictError(
                "checklist_binding_scope_unsupported",
                retryable=False,
            )
        persistence = uow.services.checklists
        previous = await persistence.get_binding(
            board_id=command.board_id,
            target_type=command.target_type,
            phase=command.phase,
        )
        current_revision = 0 if previous is None else previous.revision
        if command.expected_revision != current_revision:
            raise ChecklistConflictError("checklist_binding_conflict")
        service = ChecklistService()
        binding = service.prepare_binding(
            board_id=command.board_id,
            mode=command.mode,
            current_binding=previous,
        )
        result = await service.apply_binding(
            binding,
            previous_binding=previous,
            persistence=persistence,
        )
        await uow.services.boards.record_checklist_binding_change(
            board_id=command.board_id,
            actor_id=actor.actor_id,
            binding=result,
            previous_binding=previous,
            change_source="board_governance",
        )
        await commit(uow)
        return result


@dataclass(frozen=True, slots=True)
class StartChecklistExecutionCommand:
    board_id: str
    spec_id: str
    expected_spec_version: int
    spec_edition: int | None = None
    binding_version: int | None = None
    # Legacy transport aliases. Canonical transports use ``spec_edition`` and
    # ``binding_version`` and let Core derive idempotency.
    expected_spec_edition: int | None = None
    idempotency_key: str | None = None
    binding_digest: str | None = None

    def __post_init__(self) -> None:
        canonical = self.spec_edition
        legacy = self.expected_spec_edition
        if canonical is not None and legacy is not None and canonical != legacy:
            raise ValueError("checklist_spec_edition_alias_conflict")
        resolved = canonical if canonical is not None else legacy
        if not isinstance(resolved, int) or isinstance(resolved, bool) or resolved < 1:
            raise ValueError("checklist_spec_edition_invalid")
        object.__setattr__(self, "spec_edition", resolved)
        object.__setattr__(self, "expected_spec_edition", resolved)
        if self.binding_version is not None and (
            not isinstance(self.binding_version, int)
            or isinstance(self.binding_version, bool)
            or self.binding_version < 1
        ):
            raise ValueError("checklist_binding_version_invalid")


class StartChecklistExecutionUseCase:
    async def execute(
        self,
        command: StartChecklistExecutionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ChecklistExecutionStartResult:
        await _require_board(
            uow,
            board_id=command.board_id,
            actor=actor,
            write=True,
        )
        await _require_spec(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        await _require_permissions(
            uow.services,
            actor.actor_id,
            command.board_id,
            _EXECUTE_PERMISSIONS,
        )
        preflight = await _preflight(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        if preflight.subject.spec_version != command.expected_spec_version:
            raise ChecklistConflictError("checklist_spec_version_conflict")
        if preflight.subject.spec_edition != command.spec_edition:
            raise ChecklistConflictError(
                "checklist_spec_edition_conflict",
                details={
                    "expected": command.spec_edition,
                    "current": preflight.subject.spec_edition,
                },
            )
        if (
            command.binding_version is not None
            and command.binding_version != preflight.binding.version
        ):
            raise ChecklistConflictError("checklist_binding_conflict")
        if (
            command.binding_digest is not None
            and command.binding_digest != preflight.binding.digest
        ):
            raise ChecklistConflictError("checklist_binding_conflict")
        idempotency_key = command.idempotency_key or canonical_sha256(
            {
                "operation": "start_checklist_execution",
                "board_id": command.board_id,
                "spec_id": command.spec_id,
                "spec_edition": command.spec_edition,
                "expected_spec_version": command.expected_spec_version,
                "binding_version": preflight.binding.version,
            }
        )
        result = await ChecklistService().start_execution(
            preflight=preflight,
            actor_id=actor.actor_id,
            idempotency_key=idempotency_key,
            persistence=uow.services.checklists,
        )
        if not result.replayed:
            await commit(uow)
        return result


@dataclass(frozen=True, slots=True)
class SubmitChecklistExecutionCommand:
    board_id: str
    spec_id: str
    execution_id: str
    spec_edition: int
    expected_spec_version: int
    item_results: tuple[ChecklistItemResult, ...]
    # Legacy aliases are accepted only at the application boundary.
    expected_execution_revision: int | None = None
    items: tuple[ChecklistItemResult, ...] | None = None
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        canonical_items = tuple(self.item_results)
        if self.items is not None:
            legacy_items = tuple(self.items)
            if canonical_items and canonical_items != legacy_items:
                raise ValueError("checklist_item_results_alias_conflict")
            if not canonical_items:
                canonical_items = legacy_items
        object.__setattr__(self, "item_results", canonical_items)
        object.__setattr__(self, "items", canonical_items)


class SubmitChecklistExecutionUseCase:
    async def execute(
        self,
        command: SubmitChecklistExecutionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ChecklistCommitResult:
        await _require_board(
            uow,
            board_id=command.board_id,
            actor=actor,
            write=True,
        )
        await _require_spec(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        await _require_permissions(
            uow.services,
            actor.actor_id,
            command.board_id,
            _EXECUTE_PERMISSIONS,
        )
        persistence = uow.services.checklists
        execution = await persistence.get_execution(
            board_id=command.board_id,
            spec_id=command.spec_id,
            execution_id=command.execution_id,
        )
        if execution is None:
            raise EntityNotFoundError("checklist_execution", command.execution_id)
        if execution.spec_edition != command.spec_edition:
            raise ChecklistConflictError(
                "checklist_spec_edition_conflict",
                details={
                    "expected": command.spec_edition,
                    "current": execution.spec_edition,
                },
            )
        if execution.spec_version != command.expected_spec_version:
            raise ChecklistConflictError("checklist_spec_version_conflict")
        expected_execution_revision = (
            execution.revision - 1
            if execution.status is ChecklistExecutionStatus.SUBMITTED
            else execution.revision
        )
        if (
            command.expected_execution_revision is not None
            and command.expected_execution_revision
            != expected_execution_revision
        ):
            raise ChecklistConflictError(
                "checklist_execution_revision_conflict"
            )
        idempotency_key = command.idempotency_key or canonical_sha256(
            {
                "operation": "submit_checklist_execution",
                "board_id": command.board_id,
                "spec_id": command.spec_id,
                "spec_edition": command.spec_edition,
                "expected_spec_version": command.expected_spec_version,
                "execution_id": command.execution_id,
                "item_results": [
                    {
                        "item_id": item.item_id,
                        "outcome": item.outcome.value,
                        "anchor": item.anchor,
                        "rationale": item.rationale,
                    }
                    for item in command.item_results
                ],
            }
        )
        if execution.status is ChecklistExecutionStatus.SUBMITTED:
            if (
                execution.receipt_id is None
                or expected_execution_revision != execution.revision - 1
            ):
                raise ChecklistConflictError(
                    "checklist_execution_revision_conflict"
                )
            receipt = await persistence.get_receipt(
                board_id=command.board_id,
                receipt_id=execution.receipt_id,
            )
            if receipt is None:
                raise ChecklistConflictError(
                    "checklist_execution_state_conflict",
                    retryable=False,
                )
            replay_submission = ChecklistSubmission(
                board_id=execution.board_id,
                spec_id=execution.spec_id,
                spec_version=execution.spec_version,
                content_digest=execution.content_digest,
                input_digest=execution.input_digest,
                template_version=execution.template_version,
                template_digest=execution.template_digest,
                binding_version=execution.binding_version,
                binding_digest=execution.binding_digest,
                expected_head_revision=receipt.head_revision - 1,
                items=command.item_results,
                idempotency_key=idempotency_key,
                spec_edition=execution.spec_edition,
            )
            return ChecklistService().resolve_replay(
                replay_submission,
                actor_id=actor.actor_id,
                result=ChecklistCommitResult(
                    board_id=receipt.board_id,
                    spec_id=receipt.spec_id,
                    spec_version=receipt.spec_version,
                    receipt_id=receipt.id,
                    request_digest=receipt.request_digest,
                    head_revision=receipt.head_revision,
                    replayed=True,
                    spec_edition=receipt.spec_edition,
                ),
            )
        preflight = await _preflight(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        submission = ChecklistSubmission(
            board_id=execution.board_id,
            spec_id=execution.spec_id,
            spec_version=execution.spec_version,
            content_digest=execution.content_digest,
            input_digest=execution.input_digest,
            template_version=execution.template_version,
            template_digest=execution.template_digest,
            binding_version=execution.binding_version,
            binding_digest=execution.binding_digest,
            expected_head_revision=preflight.current_head_revision,
            items=command.item_results,
            idempotency_key=idempotency_key,
            spec_edition=execution.spec_edition,
        )
        result = await ChecklistService().submit_started_execution(
            execution,
            submission,
            expected_execution_revision=expected_execution_revision,
            actor_id=actor.actor_id,
            preflight=preflight,
            persistence=persistence,
        )
        if not result.replayed:
            await commit(uow)
        return result


@dataclass(frozen=True, slots=True)
class GetChecklistBindingCommand:
    board_id: str


class GetChecklistBindingUseCase:
    async def execute(
        self,
        command: GetChecklistBindingCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ChecklistBinding:
        await _require_board(
            uow,
            board_id=command.board_id,
            actor=actor,
            write=False,
        )
        await _require_permissions(
            uow.services,
            actor.actor_id,
            command.board_id,
            _READ_PERMISSIONS,
        )
        binding = await uow.services.checklists.get_binding(
            board_id=command.board_id,
            target_type=ChecklistTargetType.SPEC,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
        return binding or ChecklistBinding.synthetic_off(
            board_id=command.board_id,
        )


@dataclass(frozen=True, slots=True)
class GetChecklistReceiptCommand:
    board_id: str
    receipt_id: str


class GetChecklistReceiptUseCase:
    async def execute(
        self,
        command: GetChecklistReceiptCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ChecklistReceipt:
        await _require_board(
            uow,
            board_id=command.board_id,
            actor=actor,
            write=False,
        )
        await _require_permissions(
            uow.services,
            actor.actor_id,
            command.board_id,
            _READ_PERMISSIONS,
        )
        return await ChecklistService().get_receipt(
            board_id=command.board_id,
            receipt_id=command.receipt_id,
            persistence=uow.services.checklists,
        )


@dataclass(frozen=True, slots=True)
class ChecklistSpecState:
    subject: ChecklistSpecSnapshot
    binding: ChecklistBinding
    current_receipt: ChecklistReceipt | None
    currentness: ChecklistCurrentness | None
    gate: ChecklistGateDecision


@dataclass(frozen=True, slots=True)
class GetChecklistSpecStateCommand:
    board_id: str
    spec_id: str


class GetChecklistSpecStateUseCase:
    async def execute(
        self,
        command: GetChecklistSpecStateCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ChecklistSpecState:
        await _require_board(
            uow,
            board_id=command.board_id,
            actor=actor,
            write=False,
        )
        await _require_spec(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        await _require_permissions(
            uow.services,
            actor.actor_id,
            command.board_id,
            _READ_PERMISSIONS,
        )
        preflight = await _preflight(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        service = ChecklistService()
        gate = await service.evaluate_spec_gate(
            board_id=command.board_id,
            spec_id=command.spec_id,
            persistence=uow.services.checklists,
        )
        if preflight.current_head_receipt_id is None:
            return ChecklistSpecState(
                subject=preflight.subject,
                binding=preflight.binding,
                current_receipt=None,
                currentness=None,
                gate=gate,
            )
        current = await service.get_current(
            board_id=command.board_id,
            spec_id=command.spec_id,
            current_subject=preflight.subject,
            current_binding=preflight.binding,
            persistence=uow.services.checklists,
        )
        return ChecklistSpecState(
            subject=preflight.subject,
            binding=preflight.binding,
            current_receipt=current.receipt,
            currentness=current.currentness,
            gate=current.gate,
        )


@dataclass(frozen=True, slots=True)
class ListChecklistExecutionsCommand:
    board_id: str
    spec_id: str
    offset: int = 0
    limit: int = 25
    state: ChecklistReceiptState | None = None


class ListChecklistExecutionsUseCase:
    async def execute(
        self,
        command: ListChecklistExecutionsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ChecklistPage[ChecklistReceiptView]:
        await _require_board(
            uow,
            board_id=command.board_id,
            actor=actor,
            write=False,
        )
        await _require_spec(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        await _require_permissions(
            uow.services,
            actor.actor_id,
            command.board_id,
            _READ_PERMISSIONS,
        )
        preflight = await _preflight(
            uow,
            board_id=command.board_id,
            spec_id=command.spec_id,
        )
        return await ChecklistService().list_executions(
            ChecklistListQuery(
                board_id=command.board_id,
                spec_id=command.spec_id,
                offset=command.offset,
                limit=command.limit,
                current_spec_edition=preflight.subject.spec_edition,
                state=command.state,
            ),
            current_subject=preflight.subject,
            current_binding=preflight.binding,
            head_receipt_id=preflight.current_head_receipt_id,
            persistence=uow.services.checklists,
        )


class ListChecklistTemplatesUseCase:
    @staticmethod
    async def execute() -> tuple[ChecklistTemplate, ...]:
        return (SPECIFY_CHECKLIST_TEMPLATE_V1,)


@dataclass(frozen=True, slots=True)
class GetChecklistTemplateCommand:
    template_version: str


class GetChecklistTemplateUseCase:
    @staticmethod
    async def execute(command: GetChecklistTemplateCommand) -> ChecklistTemplate:
        if command.template_version != SPECIFY_CHECKLIST_TEMPLATE_V1.version:
            raise EntityNotFoundError(
                "checklist_template",
                command.template_version,
            )
        return SPECIFY_CHECKLIST_TEMPLATE_V1


__all__ = [
    "GetChecklistBindingCommand",
    "GetChecklistBindingUseCase",
    "GetChecklistReceiptCommand",
    "GetChecklistReceiptUseCase",
    "GetChecklistSpecStateCommand",
    "GetChecklistSpecStateUseCase",
    "GetChecklistTemplateCommand",
    "GetChecklistTemplateUseCase",
    "ChecklistSpecState",
    "ListChecklistExecutionsCommand",
    "ListChecklistExecutionsUseCase",
    "ListChecklistTemplatesUseCase",
    "StartChecklistExecutionCommand",
    "StartChecklistExecutionUseCase",
    "SubmitChecklistExecutionCommand",
    "SubmitChecklistExecutionUseCase",
    "UpdateChecklistBindingCommand",
    "UpdateChecklistBindingUseCase",
]
