"""Permission-aware consistent read for renderer-neutral entity exports."""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
    resolve_actor_permissions,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.entity_export import (
    EntityExportBundle,
    EntityExportDisclosure,
    EntityExportHistoryScope,
    EntityExportRequest,
    EntityExportType,
)
from okto_pulse.core.domain.permissions import ALL_FLAGS
from okto_pulse.core.ports.entity_export import EntityExportReadPort
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


ENTITY_EXPORT_ROOT_READ_PERMISSION: dict[EntityExportType, str] = {
    EntityExportType.STORY: "story.entity.read",
    EntityExportType.IDEATION: "ideation.entity.read",
    EntityExportType.REFINEMENT: "refinement.entity.read",
    EntityExportType.SPEC: "spec.entity.read",
    EntityExportType.SPRINT: "sprint.entity.read",
    EntityExportType.CARD: "card.entity.read",
    EntityExportType.TEST_SCENARIO: "spec.tests.read",
}


@dataclass(frozen=True, slots=True)
class GetEntityExportBundleCommand:
    board_id: str
    entity_type: EntityExportType
    entity_id: str
    history_scope: EntityExportHistoryScope = EntityExportHistoryScope.COMPLETE
    requested_sections: tuple[str, ...] = ()

    def to_request(self) -> EntityExportRequest:
        return EntityExportRequest(
            board_id=self.board_id,
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            history_scope=self.history_scope,
            requested_sections=self.requested_sections,
        )


@dataclass(frozen=True, slots=True)
class GetEntityExportBundleResult:
    bundle: EntityExportBundle


def _resolved_disclosure(
    *,
    actor: ActorContext,
    permissions: object,
    requested_sections: tuple[str, ...],
) -> EntityExportDisclosure:
    # Materialize closed decisions once. Edition readers receive no policy
    # object and cannot accidentally reinterpret absent legacy flags.
    granted = frozenset(
        permission
        for permission in ALL_FLAGS
        if decide_authorization(
            actor,
            PermissionRequirement(permission),
            permissions=permissions,
        ).allowed
    )
    return EntityExportDisclosure(
        granted_permissions=granted,
        requested_sections=requested_sections,
    )


class GetEntityExportBundleUseCase:
    """Read one complete export projection from a single authorized snapshot."""

    async def execute(
        self,
        command: GetEntityExportBundleCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetEntityExportBundleResult:
        request = command.to_request()

        # This must be the first physical UoW operation. The board/access
        # preflight, permission resolution and every section then observe the
        # same transaction-wide snapshot.
        await uow.begin_consistent_read()
        if await load_accessible_board(uow, request.board_id, actor) is None:
            raise EntityNotFoundError(request.entity_type.value, request.entity_id)

        permissions = await resolve_actor_permissions(actor, uow, request.board_id)
        root_permission = ENTITY_EXPORT_ROOT_READ_PERMISSION[request.entity_type]
        if not decide_authorization(
            actor,
            PermissionRequirement(root_permission),
            permissions=permissions,
        ).allowed:
            # Root denial and a missing entity are intentionally
            # indistinguishable and the reader has not probed the entity yet.
            raise EntityNotFoundError(request.entity_type.value, request.entity_id)

        reader = getattr(uow, "entity_exports", None)
        if not isinstance(reader, EntityExportReadPort):
            raise TypeError("entity_export_reader_adapter_missing")
        bundle = await reader.build_bundle(
            request=request,
            disclosure=_resolved_disclosure(
                actor=actor,
                permissions=permissions,
                requested_sections=request.requested_sections,
            ),
            actor_id=actor.actor_id,
            realm_scope=uow.realm_scope,
        )
        subject = bundle.subject
        if (
            subject.board_id != request.board_id
            or subject.entity_type is not request.entity_type
            or subject.entity_id != request.entity_id
            or bundle.history_scope is not request.history_scope
        ):
            raise TypeError("entity_export_reader_subject_mismatch")
        return GetEntityExportBundleResult(bundle=bundle)


__all__ = [
    "ENTITY_EXPORT_ROOT_READ_PERMISSION",
    "GetEntityExportBundleCommand",
    "GetEntityExportBundleResult",
    "GetEntityExportBundleUseCase",
]
