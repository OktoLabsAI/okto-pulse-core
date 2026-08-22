"""Shared REST/MCP use cases for the Refinement Research Decision Ledger."""

from __future__ import annotations

from dataclasses import dataclass, replace

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.enums import RefinementStatus
from okto_pulse.core.domain.permissions import Permissions
from okto_pulse.core.domain.research_decision_ledger import (
    RefinementLedgerContext,
    ResearchDecisionCommitResult,
    ResearchDecisionContent,
    ResearchDecisionCursor,
    ResearchDecisionDerivation,
    ResearchDecisionEntry,
    ResearchDecisionHead,
    ResearchDecisionLedgerSnapshot,
    ResearchDecisionOffsetPage,
    ResearchDecisionPage,
    ResearchDecisionStatus,
)
from okto_pulse.core.ports.research_decision_ledger import (
    ResearchDecisionListQuery,
    ResearchDecisionOffsetListQuery,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.permission_policy import check_permission
from okto_pulse.core.services.research_decision_ledger import (
    AppendResearchDecisionCommand,
    ResearchDecisionLedgerService,
    SupersedeResearchDecisionCommand,
)

_RDL_READ_PERMISSION = "refinement.research_decisions.read"
_RDL_APPEND_PERMISSION = "refinement.research_decisions.append"
_WRITE_SHARE_PERMISSIONS = {"editor", "admin"}


@dataclass(frozen=True, slots=True)
class WriteResearchDecisionCommand:
    board_id: str | None
    refinement_id: str
    expected_refinement_version: int | None
    expected_head_revision: int
    content: ResearchDecisionContent
    idempotency_key: str
    ledger_id: str | None = None
    supersedes_entry_id: str | None = None

    @property
    def operation(self) -> str:
        return "supersede" if self.supersedes_entry_id is not None else "append"


@dataclass(frozen=True, slots=True)
class WriteResearchDecisionResult:
    receipt: ResearchDecisionCommitResult


@dataclass(frozen=True, slots=True)
class ListResearchDecisionsCommand:
    board_id: str | None
    refinement_id: str
    limit: int = 50
    cursor: ResearchDecisionCursor | None = None
    ledger_id: str | None = None
    status: ResearchDecisionStatus | None = None


@dataclass(frozen=True, slots=True)
class ListResearchDecisionsResult:
    page: ResearchDecisionPage[ResearchDecisionEntry]


@dataclass(frozen=True, slots=True)
class ListResearchDecisionsRestCommand:
    board_id: str | None
    refinement_id: str
    offset: int = 0
    limit: int = 25
    ledger_id: str | None = None
    status: ResearchDecisionStatus | None = None


@dataclass(frozen=True, slots=True)
class ListResearchDecisionsRestResult:
    page: ResearchDecisionOffsetPage[ResearchDecisionEntry]


@dataclass(frozen=True, slots=True)
class GetResearchDecisionHeadCommand:
    board_id: str | None
    refinement_id: str
    ledger_id: str


@dataclass(frozen=True, slots=True)
class GetResearchDecisionHeadResult:
    entry: ResearchDecisionEntry
    head: ResearchDecisionHead


async def _require_refinement(
    *,
    command_board_id: str | None,
    refinement_id: str,
    actor: ActorContext,
    uow: PulseUnitOfWork,
    write: bool,
):
    refinement = await uow.services.refinements.get_refinement(refinement_id)
    if (
        refinement is None
        or (
            command_board_id is not None
            and refinement.board_id != command_board_id
        )
        or (
            actor.board_id is not None
            and actor.board_id != refinement.board_id
        )
    ):
        raise EntityNotFoundError("refinement", refinement_id)
    if actor.source == "mcp" and actor.board_id == refinement.board_id:
        return refinement
    board = await load_accessible_board(
        uow,
        refinement.board_id,
        actor,
        allowed_share_permissions=_WRITE_SHARE_PERMISSIONS if write else None,
    )
    if board is None:
        raise EntityNotFoundError("refinement", refinement_id)
    return refinement


async def _require_permissions(
    *,
    actor: ActorContext,
    uow: PulseUnitOfWork,
    board_id: str,
    permissions: tuple[str, ...],
) -> None:
    permission_set = actor.permissions
    if actor.source != "mcp":
        permission_set = await uow.services.resolve_user_permissions(
            actor.actor_id,
            board_id,
        )
    for permission in permissions:
        error = check_permission(permission_set, permission)
        if error:
            raise PermissionDeniedError(error)


class WriteResearchDecisionUseCase:
    """Prepare and atomically persist one append or supersede operation."""

    async def execute(
        self,
        command: WriteResearchDecisionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> WriteResearchDecisionResult:
        if not isinstance(command, WriteResearchDecisionCommand):
            raise ValueError("research_decision_write_command_invalid")
        if not command.idempotency_key.strip():
            raise ValueError("research_decision_idempotency_key_required")
        refinement = await _require_refinement(
            command_board_id=command.board_id,
            refinement_id=command.refinement_id,
            actor=actor,
            uow=uow,
            write=True,
        )
        board_id = refinement.board_id
        if command.operation == "append" and (
            command.expected_head_revision != 0
            or command.ledger_id is not None
        ):
            raise ValueError("research_decision_append_fences_invalid")
        if command.operation == "supersede" and (
            command.expected_head_revision < 1
            or command.ledger_id is None
        ):
            raise ValueError("research_decision_supersede_fences_required")
        await _require_permissions(
            actor=actor,
            uow=uow,
            board_id=board_id,
            permissions=(Permissions.SPECS_UPDATE, _RDL_APPEND_PERMISSION),
        )
        service = ResearchDecisionLedgerService()
        expected_refinement_version = (
            int(refinement.version)
            if command.expected_refinement_version is None
            else command.expected_refinement_version
        )
        request_digest = service.request_digest(
            board_id=board_id,
            refinement_id=command.refinement_id,
            expected_refinement_version=command.expected_refinement_version,
            content=command.content,
            actor_id=actor.actor_id,
            ledger_id=command.ledger_id,
            expected_head_revision=command.expected_head_revision,
            predecessor_entry_id=command.supersedes_entry_id,
        )
        persistence = uow.services.research_decisions
        replay = await persistence.resolve_idempotent_result(
            board_id=board_id,
            refinement_id=command.refinement_id,
            idempotency_key=command.idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            return WriteResearchDecisionResult(replay)

        context = RefinementLedgerContext(
            board_id=refinement.board_id,
            refinement_id=refinement.id,
            version=refinement.version,
            status=(
                refinement.status
                if isinstance(refinement.status, RefinementStatus)
                else RefinementStatus(refinement.status)
            ),
            archived=bool(refinement.archived),
        )
        if command.operation == "append":
            bundle = service.prepare_append(
                AppendResearchDecisionCommand(
                    board_id=board_id,
                    refinement_id=command.refinement_id,
                    expected_refinement_version=(
                        expected_refinement_version
                    ),
                    expected_head_revision=command.expected_head_revision,
                    content=command.content,
                    actor_id=actor.actor_id,
                    idempotency_key=command.idempotency_key,
                    actor_type=(
                        "agent" if actor.source == "mcp" else "user"
                    ),
                ),
                context=context,
            )
        else:
            if command.ledger_id is None:
                raise ValueError("research_decision_ledger_id_required")
            if command.supersedes_entry_id is None:
                raise ValueError(
                    "research_decision_predecessor_entry_id_required"
                )
            current = await persistence.get_current(
                board_id=board_id,
                refinement_id=command.refinement_id,
                ledger_id=command.ledger_id,
            )
            if current is None:
                raise EntityNotFoundError(
                    "research_decision",
                    command.ledger_id,
                )
            predecessor, head = current
            bundle = service.prepare_supersede(
                SupersedeResearchDecisionCommand(
                    board_id=board_id,
                    refinement_id=command.refinement_id,
                    ledger_id=command.ledger_id,
                    predecessor_entry_id=command.supersedes_entry_id,
                    expected_refinement_version=(
                        expected_refinement_version
                    ),
                    expected_head_revision=command.expected_head_revision,
                    content=command.content,
                    actor_id=actor.actor_id,
                    idempotency_key=command.idempotency_key,
                    actor_type=(
                        "agent" if actor.source == "mcp" else "user"
                    ),
                ),
                context=context,
                current_head=head,
                predecessor=predecessor,
            )
        if command.expected_refinement_version is None:
            bundle = replace(bundle, request_digest=request_digest)
        receipt = await persistence.apply_bundle_cas(bundle)
        await commit(uow)
        return WriteResearchDecisionResult(receipt)


class ListResearchDecisionsUseCase:
    """Authorize and return one bounded keyset page."""

    async def execute(
        self,
        command: ListResearchDecisionsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListResearchDecisionsResult:
        if not isinstance(command, ListResearchDecisionsCommand):
            raise ValueError("research_decision_list_command_invalid")
        refinement = await _require_refinement(
            command_board_id=command.board_id,
            refinement_id=command.refinement_id,
            actor=actor,
            uow=uow,
            write=False,
        )
        board_id = refinement.board_id
        await _require_permissions(
            actor=actor,
            uow=uow,
            board_id=board_id,
            permissions=(Permissions.BOARD_READ, _RDL_READ_PERMISSION),
        )
        page = await uow.services.research_decisions.list_entries(
            ResearchDecisionListQuery(
                board_id=board_id,
                refinement_id=command.refinement_id,
                limit=command.limit,
                cursor=command.cursor,
                ledger_id=command.ledger_id,
                status=command.status,
            )
        )
        return ListResearchDecisionsResult(page)


class ListResearchDecisionsRestUseCase:
    """Authorize and return one exact-total REST offset page."""

    async def execute(
        self,
        command: ListResearchDecisionsRestCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListResearchDecisionsRestResult:
        if not isinstance(command, ListResearchDecisionsRestCommand):
            raise ValueError("research_decision_list_command_invalid")
        refinement = await _require_refinement(
            command_board_id=command.board_id,
            refinement_id=command.refinement_id,
            actor=actor,
            uow=uow,
            write=False,
        )
        board_id = refinement.board_id
        await _require_permissions(
            actor=actor,
            uow=uow,
            board_id=board_id,
            permissions=(Permissions.BOARD_READ, _RDL_READ_PERMISSION),
        )
        page = await uow.services.research_decisions.list_entries_offset(
            ResearchDecisionOffsetListQuery(
                board_id=board_id,
                refinement_id=command.refinement_id,
                offset=command.offset,
                limit=command.limit,
                ledger_id=command.ledger_id,
                status=command.status,
            )
        )
        return ListResearchDecisionsRestResult(page)


class GetResearchDecisionHeadUseCase:
    """Authorize and return the authoritative CAS head for one RDL ledger."""

    async def execute(
        self,
        command: GetResearchDecisionHeadCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetResearchDecisionHeadResult:
        if not isinstance(command, GetResearchDecisionHeadCommand):
            raise ValueError("research_decision_head_command_invalid")
        if not isinstance(command.ledger_id, str) or not command.ledger_id.strip():
            raise ValueError("research_decision_ledger_id_invalid")
        refinement = await _require_refinement(
            command_board_id=command.board_id,
            refinement_id=command.refinement_id,
            actor=actor,
            uow=uow,
            write=False,
        )
        board_id = refinement.board_id
        await _require_permissions(
            actor=actor,
            uow=uow,
            board_id=board_id,
            permissions=(Permissions.BOARD_READ, _RDL_READ_PERMISSION),
        )
        current = await uow.services.research_decisions.get_current(
            board_id=board_id,
            refinement_id=command.refinement_id,
            ledger_id=command.ledger_id.strip(),
        )
        if current is None:
            raise EntityNotFoundError("research_decision", command.ledger_id)
        entry, head = current
        return GetResearchDecisionHeadResult(entry=entry, head=head)


async def ensure_research_decision_snapshot(
    *,
    refinement,
    uow: PulseUnitOfWork,
    refinement_version: int | None = None,
    require_existing: bool = False,
) -> ResearchDecisionLedgerSnapshot:
    """Freeze current RDL heads at the authoritative Refinement version."""

    board_id = str(refinement.board_id)
    refinement_id = str(refinement.id)
    resolved_version = int(
        refinement.version if refinement_version is None else refinement_version
    )
    persistence = uow.services.research_decisions
    existing = await persistence.get_snapshot_for_version(
        board_id=board_id,
        refinement_id=refinement_id,
        refinement_version=resolved_version,
    )
    if existing is not None:
        return existing
    if require_existing:
        raise ValueError("research_decision_snapshot_required")
    status = (
        refinement.status
        if isinstance(refinement.status, RefinementStatus)
        else RefinementStatus(refinement.status)
    )
    context = RefinementLedgerContext(
        board_id=board_id,
        refinement_id=refinement_id,
        version=resolved_version,
        status=status,
        archived=bool(getattr(refinement, "archived", False)),
    )
    current = await persistence.list_current_entries_with_heads(
        board_id=board_id,
        refinement_id=refinement_id,
    )
    snapshot = ResearchDecisionLedgerService().freeze_heads(
        context=context,
        current=current,
    )
    return await persistence.save_snapshot(snapshot)


async def bind_research_decisions_to_spec(
    *,
    refinement,
    spec,
    uow: PulseUnitOfWork,
) -> ResearchDecisionDerivation:
    """Persist resolved RDL references without copying into ``Spec.decisions``."""

    if (
        str(spec.board_id) != str(refinement.board_id)
        or str(spec.refinement_id) != str(refinement.id)
    ):
        raise ValueError("research_decision_derivation_scope_mismatch")
    source_version = getattr(spec, "source_refinement_version", None)
    snapshot = await ensure_research_decision_snapshot(
        refinement=refinement,
        uow=uow,
        refinement_version=(
            int(source_version) if source_version is not None else None
        ),
        require_existing=source_version is not None,
    )
    derivation = ResearchDecisionLedgerService().derive_resolved_references(
        snapshot=snapshot,
        board_id=str(spec.board_id),
        spec_id=str(spec.id),
        spec_version=int(spec.version),
    )
    return await uow.services.research_decisions.save_derivation(derivation)


__all__ = [
    "bind_research_decisions_to_spec",
    "ensure_research_decision_snapshot",
    "GetResearchDecisionHeadCommand",
    "GetResearchDecisionHeadResult",
    "GetResearchDecisionHeadUseCase",
    "ListResearchDecisionsCommand",
    "ListResearchDecisionsResult",
    "ListResearchDecisionsRestCommand",
    "ListResearchDecisionsRestResult",
    "ListResearchDecisionsRestUseCase",
    "ListResearchDecisionsUseCase",
    "WriteResearchDecisionCommand",
    "WriteResearchDecisionResult",
    "WriteResearchDecisionUseCase",
]
