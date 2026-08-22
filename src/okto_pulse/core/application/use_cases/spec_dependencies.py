"""Transport-neutral application use cases for Spec precedence."""

from __future__ import annotations

from dataclasses import dataclass, replace

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
    require_authorization,
    resolve_actor_permissions,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.application.use_cases.mutation_permissions import entity_state
from okto_pulse.core.application.use_cases.spec_crud import _require_actor_board_spec
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.spec_dependency import (
    SpecDependencyDirection,
    SpecDependencyLifecycleFilter,
    SpecDependencyLineageFilter,
    SpecDependencyListQuery,
    SpecDependencyMutationReceipt,
    SpecDependencyOperationError,
    SpecDependencyPage,
    SpecDependencyReadiness,
    SpecDependencySatisfactionFilter,
    SpecDependencySpecSnapshot,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.spec_dependency_observability import (
    observe_spec_dependency_mutation,
)


def _actor_type(actor: ActorContext) -> str:
    if actor.actor_kind == "agent":
        return "agent"
    if actor.actor_kind == "system":
        return "system"
    # Persistence audit vocabulary predates PrincipalKind and uses ``user``
    # for authenticated humans.
    return "agent" if actor.source == "mcp" else "user"


async def _actor_name(actor: ActorContext, uow: PulseUnitOfWork, board_id: str) -> str:
    if actor.actor_name:
        return actor.actor_name
    return await uow.services.resolve_actor_name(actor.actor_id, board_id)


async def _require_dependency_permission(
    actor: ActorContext,
    uow: PulseUnitOfWork,
    spec: object,
    *,
    write: bool,
) -> None:
    await require_authorization(
        actor,
        _dependency_permission_requirement(spec, write=write),
        uow=uow,
        board_id=str(getattr(spec, "board_id")),
    )


def _dependency_permission_requirement(
    spec: object,
    *,
    write: bool,
) -> PermissionRequirement:
    operation = "spec.entity.manage_dependencies" if write else "spec.entity.read"
    return PermissionRequirement(
        operation,
        legacy_operation="specs:update" if write else None,
        entity="spec",
        state=entity_state(spec),
    )


def _authorized_dependency_target_snapshot(
    target: object,
) -> SpecDependencySpecSnapshot:
    """Freeze the exact target leaf whose visibility was authorized.

    Authorization necessarily happens before the service enters the graph
    critical section.  Passing this immutable snapshot across that boundary
    lets the service reject a target that changed before its row was locked,
    instead of applying the earlier decision to a different lifecycle leaf.
    """

    raw_status = getattr(target, "status")
    return SpecDependencySpecSnapshot(
        id=str(getattr(target, "id")),
        board_id=str(getattr(target, "board_id")),
        title=str(getattr(target, "title", "")),
        status=SpecStatus(str(getattr(raw_status, "value", raw_status))),
        edition=int(getattr(target, "edition", 1) or 1),
        version=int(getattr(target, "version", 1) or 1),
        archived=bool(getattr(target, "archived", False)),
        ideation_id=getattr(target, "ideation_id", None),
        last_started_edition=getattr(target, "last_started_edition", None),
    )


def _mask_dependency_remove_capability(page: SpecDependencyPage) -> SpecDependencyPage:
    """Defend the application boundary against an over-permissive adapter."""

    return replace(
        page,
        items=tuple(
            replace(
                item,
                capabilities=replace(
                    item.capabilities,
                    can_remove=False,
                    removal_blocked_reason=(
                        item.capabilities.removal_blocked_reason or "permission_denied"
                    ),
                ),
            )
            for item in page.items
        ),
    )


@dataclass(frozen=True, slots=True)
class AddSpecDependencyCommand:
    spec_id: str
    target_spec_id: str
    expected_spec_version: int
    expected_spec_edition: int
    idempotency_key: str
    board_id: str | None = None


@dataclass(frozen=True, slots=True)
class AddSpecDependencyResult:
    receipt: SpecDependencyMutationReceipt


class AddSpecDependencyUseCase:
    @observe_spec_dependency_mutation("add")
    async def execute(
        self,
        command: AddSpecDependencyCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> AddSpecDependencyResult:
        source = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        if command.board_id is not None and str(source.board_id) != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        # Authorization on the visible source precedes every target lookup so a
        # principal without mutation authority cannot use this operation as a
        # prerequisite-existence oracle.
        await _require_dependency_permission(actor, uow, source, write=True)
        # Dependency management does not imply entity visibility.  The source
        # and target leaves are checked independently so state-scoped presets
        # cannot mutate an endpoint they are not allowed to read.
        await _require_dependency_permission(actor, uow, source, write=False)
        try:
            target = await _require_actor_board_spec(
                uow,
                command.target_spec_id,
                actor,
                write=False,
            )
            await _require_dependency_permission(actor, uow, target, write=False)
        except (EntityNotFoundError, PermissionDeniedError) as exc:
            # AC-05: do not disclose whether a referenced prerequisite exists
            # outside the actor's visible board or leaf scope. The source
            # checks above deliberately retain their ordinary contracts.
            raise SpecDependencyOperationError(
                "dependency_target_unavailable",
                "Dependency target is unavailable.",
                facts={"spec_id": source.id},
            ) from exc
        if target.board_id != source.board_id:
            raise SpecDependencyOperationError(
                "cross_board_dependency_forbidden",
                "Operational Spec dependencies cannot cross board boundaries.",
                remediation="choose_a_prerequisite_from_the_same_board",
                facts={
                    "spec_id": source.id,
                    "target_spec_id": target.id,
                },
            )
        receipt = await uow.services.spec_dependencies.add_dependency(
            board_id=source.board_id,
            source_spec_id=source.id,
            target_spec_id=target.id,
            expected_spec_version=command.expected_spec_version,
            expected_spec_edition=command.expected_spec_edition,
            idempotency_key=command.idempotency_key.strip(),
            actor_id=actor.actor_id,
            actor_type=_actor_type(actor),
            actor_name=await _actor_name(actor, uow, source.board_id),
            expected_spec_status=source.status,
            authorized_target_snapshot=_authorized_dependency_target_snapshot(target),
        )
        await commit(uow)
        return AddSpecDependencyResult(receipt)


@dataclass(frozen=True, slots=True)
class RemoveSpecDependencyCommand:
    spec_id: str
    dependency_id: str
    reason: str
    expected_spec_version: int
    expected_spec_edition: int
    idempotency_key: str
    board_id: str | None = None


@dataclass(frozen=True, slots=True)
class RemoveSpecDependencyResult:
    receipt: SpecDependencyMutationReceipt


class RemoveSpecDependencyUseCase:
    @observe_spec_dependency_mutation("remove")
    async def execute(
        self,
        command: RemoveSpecDependencyCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> RemoveSpecDependencyResult:
        source = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        if command.board_id is not None and str(source.board_id) != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        await _require_dependency_permission(actor, uow, source, write=True)
        # Mutation authority does not grant visibility. Keep remove symmetric
        # with add so a principal cannot operate on a hidden source by UUID and
        # recover dependency metadata from the mutation receipt.
        await _require_dependency_permission(actor, uow, source, write=False)
        receipt = await uow.services.spec_dependencies.remove_dependency(
            board_id=source.board_id,
            source_spec_id=source.id,
            dependency_id=command.dependency_id,
            reason=command.reason,
            expected_spec_version=command.expected_spec_version,
            expected_spec_edition=command.expected_spec_edition,
            idempotency_key=command.idempotency_key.strip(),
            actor_id=actor.actor_id,
            actor_type=_actor_type(actor),
            actor_name=await _actor_name(actor, uow, source.board_id),
            expected_spec_status=source.status,
        )
        await commit(uow)
        return RemoveSpecDependencyResult(receipt)


@dataclass(frozen=True, slots=True)
class ListSpecDependenciesCommand:
    spec_id: str
    direction: SpecDependencyDirection
    cursor: str | None = None
    limit: int = 25
    lifecycle: SpecDependencyLifecycleFilter = SpecDependencyLifecycleFilter.ACTIVE
    satisfaction: SpecDependencySatisfactionFilter = (
        SpecDependencySatisfactionFilter.ALL
    )
    lineage: SpecDependencyLineageFilter = SpecDependencyLineageFilter.ALL
    related_statuses: tuple[SpecStatus, ...] = ()
    retrospective: bool | None = None
    # Optional transport board fence; kept last so existing positional callers
    # retain the pre-SK-M command shape.
    board_id: str | None = None


@dataclass(frozen=True, slots=True)
class ListSpecDependenciesResult:
    page: SpecDependencyPage


class ListSpecDependenciesUseCase:
    async def execute(
        self,
        command: ListSpecDependenciesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSpecDependenciesResult:
        # This page combines authorization, the anchor Spec, dependency rows,
        # exact totals and embedded readiness.  They are one human-visible
        # result and therefore must come from one snapshot.  This is the first
        # UoW operation by contract; adapters fail closed if an incompatible
        # physical transaction was already used.
        await uow.begin_consistent_read()
        spec = await _require_actor_board_spec(uow, command.spec_id, actor)
        if command.board_id is not None and str(spec.board_id) != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        board_id = str(getattr(spec, "board_id"))
        permissions = await resolve_actor_permissions(actor, uow, board_id)
        await require_authorization(
            actor,
            _dependency_permission_requirement(spec, write=False),
            uow=uow,
            board_id=board_id,
            permissions=permissions,
        )
        can_manage_dependencies = decide_authorization(
            actor,
            _dependency_permission_requirement(spec, write=True),
            permissions=permissions,
        ).allowed
        page = await uow.services.spec_dependencies.list_page(
            SpecDependencyListQuery(
                board_id=spec.board_id,
                spec_id=spec.id,
                direction=command.direction,
                cursor=command.cursor,
                limit=command.limit,
                lifecycle=command.lifecycle,
                satisfaction=command.satisfaction,
                lineage=command.lineage,
                related_statuses=command.related_statuses,
                retrospective=command.retrospective,
                can_manage_dependencies=can_manage_dependencies,
            )
        )
        if not can_manage_dependencies:
            page = _mask_dependency_remove_capability(page)
        return ListSpecDependenciesResult(page)


@dataclass(frozen=True, slots=True)
class GetSpecDependencyReadinessCommand:
    spec_id: str


@dataclass(frozen=True, slots=True)
class GetSpecDependencyReadinessResult:
    readiness: SpecDependencyReadiness


class GetSpecDependencyReadinessUseCase:
    async def execute(
        self,
        command: GetSpecDependencyReadinessCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetSpecDependencyReadinessResult:
        await uow.begin_consistent_read()
        spec = await _require_actor_board_spec(uow, command.spec_id, actor)
        await _require_dependency_permission(actor, uow, spec, write=False)
        readiness = await uow.services.spec_dependencies.get_readiness(
            board_id=spec.board_id,
            spec_id=spec.id,
        )
        return GetSpecDependencyReadinessResult(readiness)


__all__ = [
    "AddSpecDependencyCommand",
    "AddSpecDependencyResult",
    "AddSpecDependencyUseCase",
    "GetSpecDependencyReadinessCommand",
    "GetSpecDependencyReadinessResult",
    "GetSpecDependencyReadinessUseCase",
    "ListSpecDependenciesCommand",
    "ListSpecDependenciesResult",
    "ListSpecDependenciesUseCase",
    "RemoveSpecDependencyCommand",
    "RemoveSpecDependencyResult",
    "RemoveSpecDependencyUseCase",
]
