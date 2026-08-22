"""Admin/catalog REST use cases for AF35-S3 C3.

These use cases keep the REST handlers transport-only while preserving the
existing service/orchestrator behavior for amendment revisions, default board
configuration, design systems and screen mockups.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

import hashlib
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.application.use_cases._service_payload import ServicePayload
from okto_pulse.core.application.use_cases.board_access import (
    load_accessible_board,
    load_accessible_card,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_any_authority,
    require_authorization,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.domain.human_validation_cycle import require_draft_mutation
from okto_pulse.core.services.default_board_configuration import (
    guideline_ref_diff_has_changes,
)


class DataResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class ScreenMockupUseCaseError(Exception):
    def __init__(self, status_code: int, detail: Any) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(str(detail))


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> Any:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


_BOARD_WRITE_SHARE_PERMISSIONS = {"editor", "admin"}
_GLOBAL_CATALOG_WRITE_PERMISSIONS = (
    "default_board_config.write",
    "default.board_config.write",
    "admin.catalog.write",
)


def _permission_enabled(permissions: Any, required: str) -> bool:
    if isinstance(permissions, Mapping):
        if permissions.get("*") is True or permissions.get(required) is True:
            return True
        cursor: Any = permissions
        for part in required.split("."):
            if not isinstance(cursor, Mapping) or part not in cursor:
                return False
            cursor = cursor[part]
        return cursor is True
    checker = getattr(permissions, "check", None)
    if callable(checker):
        try:
            return checker(required) is None
        except Exception:
            return False
    if isinstance(permissions, (list, tuple, set, frozenset)):
        return required in permissions or "*" in permissions
    return False


def require_global_catalog_admin(actor: ActorContext) -> None:
    """Require an authenticated administrative role or explicit capability."""

    roles = {str(role).lower() for role in actor.roles}
    if roles.intersection({"admin", "operator"}) or any(
        _permission_enabled(actor.permissions, permission)
        for permission in _GLOBAL_CATALOG_WRITE_PERMISSIONS
    ):
        return
    raise PermissionDeniedError(
        "Global default-board configuration write requires an admin or operator capability"
    )


async def _has_board_access(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool,
) -> bool:
    return (
        await load_accessible_board(
            uow,
            board_id,
            actor,
            allowed_share_permissions=(
                _BOARD_WRITE_SHARE_PERMISSIONS if write else None
            ),
        )
        is not None
    )


# --- amendment revisions ----------------------------------------------------


@dataclass(frozen=True)
class CreateAmendmentRevisionCommand:
    board_id: str
    bug_id: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class ListAmendmentRevisionsCommand:
    board_id: str
    bug_id: str


@dataclass(frozen=True)
class GetAmendmentRevisionCommand:
    board_id: str
    bug_id: str
    amendment_id: str


@dataclass(frozen=True)
class AssociateAmendmentRevisionCommand:
    board_id: str
    bug_id: str
    amendment_id: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class TransitionAmendmentRevisionCommand:
    board_id: str
    bug_id: str
    amendment_id: str
    payload: dict[str, Any]


def _amendment_bug_not_found(board_id: str, bug_id: str) -> Exception:
    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
    )

    return AmendmentRevisionApiError(
        "bug_not_found",
        f"Bug '{bug_id}' was not found on this board.",
        404,
    )


def _amendment_not_found(amendment_id: str) -> Exception:
    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
    )

    return AmendmentRevisionApiError(
        "amendment_not_found",
        f"Amendment revision '{amendment_id}' was not found.",
        404,
    )


async def _require_amendment_bug_access(
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    bug_id: str,
    actor: ActorContext,
    write: bool,
) -> None:
    card = await load_accessible_card(
        uow,
        bug_id,
        actor,
        expected_board_id=board_id,
        allowed_share_permissions=(
            _BOARD_WRITE_SHARE_PERMISSIONS if write else None
        ),
    )
    if card is None:
        raise _amendment_bug_not_found(board_id, bug_id)


async def _preflight_amendment(
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    bug_id: str,
    amendment_id: str,
) -> Any:
    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
    )

    try:
        return await uow.services.amendments.get(
            board_id=board_id,
            bug_id=bug_id,
            amendment_id=amendment_id,
        )
    except AmendmentRevisionApiError as exc:
        if exc.code in {"amendment_not_found", "amendment_bug_mismatch"}:
            raise _amendment_not_found(amendment_id) from exc
        raise


class CreateAmendmentRevisionUseCase:
    async def execute(
        self, command: CreateAmendmentRevisionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await _require_amendment_bug_access(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            actor=actor,
            write=True,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "amendment.revision.create",
                legacy_operation="card.entity.edit_bug_fields",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.amendments.create(
            board_id=command.board_id,
            bug_id=command.bug_id,
            author=actor.actor_id,
            **command.payload,
        )
        await commit(uow)
        return DataResult(data)


class ListAmendmentRevisionsUseCase:
    async def execute(
        self, command: ListAmendmentRevisionsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await _require_amendment_bug_access(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            actor=actor,
            write=False,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "amendment.revision.read",
                legacy_operation="card.entity.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        return DataResult(
            await uow.services.amendments.list_for_bug(
                board_id=command.board_id, bug_id=command.bug_id
            )
        )


class GetAmendmentRevisionUseCase:
    async def execute(
        self, command: GetAmendmentRevisionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await _require_amendment_bug_access(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            actor=actor,
            write=False,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "amendment.revision.read",
                legacy_operation="card.entity.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        amendment = await _preflight_amendment(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            amendment_id=command.amendment_id,
        )
        return DataResult(amendment)


class AssociateAmendmentRevisionUseCase:
    async def execute(
        self,
        command: AssociateAmendmentRevisionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_amendment_bug_access(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            actor=actor,
            write=True,
        )
        await _preflight_amendment(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            amendment_id=command.amendment_id,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "amendment.revision.associate",
                legacy_operation="card.entity.edit_bug_fields",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.amendments.associate(
            board_id=command.board_id,
            bug_id=command.bug_id,
            amendment_id=command.amendment_id,
            actor=actor.actor_id,
            **command.payload,
        )
        await commit(uow)
        return DataResult(data)


class TransitionAmendmentRevisionUseCase:
    async def execute(
        self,
        command: TransitionAmendmentRevisionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_amendment_bug_access(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            actor=actor,
            write=True,
        )
        await _preflight_amendment(
            uow,
            board_id=command.board_id,
            bug_id=command.bug_id,
            amendment_id=command.amendment_id,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "amendment.revision.transition",
                legacy_operation="card.entity.edit_bug_fields",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.amendments.transition_lifecycle(
            board_id=command.board_id,
            bug_id=command.bug_id,
            amendment_id=command.amendment_id,
            actor=actor.actor_id,
            **command.payload,
        )
        await commit(uow)
        return DataResult(data)


# --- default board configuration -------------------------------------------


@dataclass(frozen=True)
class DefaultBoardConfigCommand:
    scope: str = "global"
    board_id: str = ""
    template_id: str = ""
    payload: dict[str, Any] | None = None


class GetActiveDefaultBoardConfigUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id or None,
        )
        return DataResult(await uow.services.default_board_config.get_active(scope=command.scope))


class ListDefaultBoardConfigVersionsUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id or None,
        )
        return DataResult(await uow.services.default_board_config.list_versions(scope=command.scope))


class CreateDefaultBoardConfigVersionUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_any_authority(
            actor,
            PermissionRequirement(
                "default_board_config.create",
                legacy_operation="spec.entity.edit_fields",
            ),
            roles=("admin", "operator"),
            uow=uow,
        )
        payload = command.payload or {}
        diff = await uow.services.default_board_config.preview_create_guideline_ref_diff(
            scope=str(payload.get("scope") or command.scope or "global"),
            guideline_default_refs=payload.get("guideline_default_refs"),
        )
        if guideline_ref_diff_has_changes(diff):
            await require_authorization(
                actor,
                PermissionRequirement(
                    "default_board_config.guidelines.edit",
                    legacy_operation="guidelines.adoption.manage",
                ),
                uow=uow,
            )
        data = await uow.services.default_board_config.create_version(
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
            **payload,
        )
        await commit(uow)
        return DataResult(data)


class ActivateDefaultBoardConfigVersionUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_any_authority(
            actor,
            PermissionRequirement(
                "default_board_config.activate",
                legacy_operation="spec.entity.edit_fields",
            ),
            roles=("admin", "operator"),
            uow=uow,
        )
        diff = await uow.services.default_board_config.preview_activate_guideline_ref_diff(
            template_id=command.template_id,
        )
        if guideline_ref_diff_has_changes(diff):
            await require_authorization(
                actor,
                PermissionRequirement(
                    "default_board_config.guidelines.edit",
                    legacy_operation="guidelines.adoption.manage",
                ),
                uow=uow,
            )
        data = await uow.services.default_board_config.activate_version(
            template_id=command.template_id,
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
        )
        await commit(uow)
        return DataResult(data)


class DeactivateDefaultBoardConfigVersionUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_any_authority(
            actor,
            PermissionRequirement(
                "default_board_config.deactivate",
                legacy_operation="spec.entity.edit_fields",
            ),
            roles=("admin", "operator"),
            uow=uow,
        )
        diff = await uow.services.default_board_config.preview_deactivate_guideline_ref_diff(
            template_id=command.template_id,
        )
        if guideline_ref_diff_has_changes(diff):
            await require_authorization(
                actor,
                PermissionRequirement(
                    "default_board_config.guidelines.edit",
                    legacy_operation="guidelines.adoption.manage",
                ),
                uow=uow,
            )
        data = await uow.services.default_board_config.deactivate_version(
            template_id=command.template_id,
            actor=actor.actor_id,
        )
        await commit(uow)
        return DataResult(data)


class GetBoardDefaultConfigDiffUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.default_board_configuration import (
            DefaultBoardConfigurationError,
        )

        if not await _has_board_access(
            uow,
            command.board_id,
            actor,
            write=False,
        ):
            raise DefaultBoardConfigurationError(
                "board_not_found",
                f"Board '{command.board_id}' was not found or is not accessible.",
                404,
            )
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.diff_read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.default_board_config.get_board_diff(
            board_id=command.board_id,
            uow=uow,
        )
        return DataResult(data)


class ListDefaultGuidelineCandidatesUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.candidates_read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id or None,
        )
        data = await uow.services.default_board_config.list_default_candidates(
            scope=command.scope,
            template_id=command.template_id or None,
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id or None),
        )
        return DataResult(data)


class UpdateDefaultGuidelineRefsUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.guidelines.edit",
                legacy_operation="guidelines.adoption.manage",
            ),
            uow=uow,
        )
        data = await uow.services.default_board_config.update_template_guidelines(
            template_id=command.template_id,
            guideline_default_refs=(command.payload or {}).get("guideline_default_refs"),
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
        )
        await commit(uow)
        return DataResult(data)


class SetDefaultDesignSystemUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await require_any_authority(
            actor,
            PermissionRequirement(
                "default_board_config.set_design_system",
                legacy_operation="spec.entity.edit_fields",
            ),
            roles=("admin", "operator"),
            uow=uow,
        )
        data = await uow.services.default_board_config.set_template_design_system(
            template_id=command.template_id,
            actor=actor.actor_id,
            **(command.payload or {}),
        )
        await commit(uow)
        return DataResult(data)


# --- design system catalog --------------------------------------------------


@dataclass(frozen=True)
class DesignSystemCommand:
    design_system_id: str = ""
    board_id: str = ""
    scope: str = "global"
    payload: dict[str, Any] | None = None
    limit: int = 50
    cursor: str | None = None
    # The same command is consumed by list and detail use cases.  ``None``
    # preserves rolling compatibility with Community adapters that predate
    # explicit projection profiles: list resolves it to ``summary`` while get
    # resolves it to ``full``.
    profile: str | None = None


def _design_system_board_not_found(board_id: str) -> Exception:
    from okto_pulse.core.services.design_system import DesignSystemError

    return DesignSystemError(
        "board_not_found",
        f"Board '{board_id}' was not found or is not accessible.",
        404,
        {"board_id": board_id},
    )


def _design_system_not_found(design_system_id: str) -> Exception:
    from okto_pulse.core.services.design_system import DesignSystemError

    return DesignSystemError(
        "design_system_not_found",
        f"Design System '{design_system_id}' was not found.",
        404,
        {"design_system_id": design_system_id},
    )


async def _require_design_system_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool,
) -> None:
    if not await _has_board_access(uow, board_id, actor, write=write):
        raise _design_system_board_not_found(board_id)


async def _require_design_system_detail_board(
    uow: PulseUnitOfWork,
    board_id: str,
    design_system_id: str,
    actor: ActorContext,
    *,
    write: bool,
) -> None:
    """Hide the board authorization outcome behind the detail resource's 404."""

    if not await _has_board_access(uow, board_id, actor, write=write):
        raise _design_system_not_found(design_system_id)


class CreateDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            serialize_design_system,
        )

        payload = command.payload or {}
        board_id: str | None = None
        if (payload.get("scope") or "global") == "inline":
            board_id = payload.get("board_id")
            if not board_id:
                # Preserve the service's structured inline-without-board error.
                board_id = ""
            if board_id:
                await _require_design_system_board(
                    uow,
                    board_id,
                    actor,
                    write=True,
                )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.entity.create",
                legacy_operation="spec.architecture.create",
            ),
            uow=uow,
            board_id=board_id or None,
        )
        item = await uow.services.design_systems.create_design_system(
            actor.actor_id,
            **payload,
        )
        await commit(uow)
        return DataResult(serialize_design_system(item))


class ListDesignSystemsUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemError,
            serialize_design_system,
        )

        # An adapter that does not send ``profile`` predates the paginated
        # envelope and advertises a list response model. Preserve that legacy
        # contract during rolling upgrades. Modern adapters always pass
        # ``summary`` and receive the bounded page below.
        if command.profile is None:
            if command.scope == "inline":
                await _require_design_system_board(
                    uow,
                    command.board_id,
                    actor,
                    write=False,
                )
            await require_authorization(
                actor,
                PermissionRequirement(
                    "design_system.entity.read",
                    legacy_operation="board.read",
                ),
                uow=uow,
                board_id=command.board_id or None,
            )
            items = await uow.services.design_systems.list_catalog(
                scope=command.scope,
                board_id=command.board_id or None,
                owner_id=actor.actor_id if command.scope == "global" else None,
            )
            return DataResult([serialize_design_system(item) for item in items])

        profile = command.profile
        if profile != "summary":
            raise DesignSystemError(
                "design_system_invalid_profile",
                "Catalog lists support only profile='summary'; use the detail endpoint for payloads.",
                422,
            )
        if command.scope == "inline":
            await _require_design_system_board(
                uow,
                command.board_id,
                actor,
                write=False,
            )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.entity.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id or None,
        )
        page = await uow.services.design_systems.list_catalog_page(
            scope=command.scope,
            board_id=command.board_id or None,
            owner_id=actor.actor_id if command.scope == "global" else None,
            limit=command.limit,
            cursor=command.cursor,
        )
        return DataResult(page)


class GetDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            serialize_design_system_profile,
        )

        board_authorized = bool(command.board_id)
        if board_authorized:
            await _require_design_system_detail_board(
                uow,
                command.board_id,
                command.design_system_id,
                actor,
                write=False,
            )
        item = await uow.services.design_systems.require_authorized_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id or None,
            board_access_authorized=board_authorized,
            allow_owned_global_without_link=True,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.entity.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id or None,
        )
        return DataResult(
            serialize_design_system_profile(item, profile=command.profile or "full")
        )


class UpdateDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            serialize_design_system,
        )

        board_authorized = bool(command.board_id)
        if board_authorized:
            await _require_design_system_detail_board(
                uow,
                command.board_id,
                command.design_system_id,
                actor,
                write=True,
            )
        await uow.services.design_systems.require_authorized_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id or None,
            board_access_authorized=board_authorized,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.entity.edit",
                legacy_operation="spec.architecture.edit",
            ),
            uow=uow,
            board_id=command.board_id or None,
        )
        item = await uow.services.design_systems.update_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id or None,
            board_access_authorized=board_authorized,
            **(command.payload or {}),
        )
        await commit(uow)
        return DataResult(serialize_design_system(item))


class DeleteDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        board_authorized = bool(command.board_id)
        if board_authorized:
            await _require_design_system_detail_board(
                uow,
                command.board_id,
                command.design_system_id,
                actor,
                write=True,
            )
        await uow.services.design_systems.require_authorized_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id or None,
            board_access_authorized=board_authorized,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.entity.delete",
                legacy_operation="spec.architecture.delete",
            ),
            uow=uow,
            board_id=command.board_id or None,
        )
        deleted = await uow.services.design_systems.delete_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id or None,
            board_access_authorized=board_authorized,
        )
        if deleted:
            await commit(uow)
        return DataResult(deleted)


class LinkBoardDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        await _require_design_system_board(
            uow,
            command.board_id,
            actor,
            write=True,
        )
        await uow.services.design_systems.require_design_system(
            (command.payload or {})["design_system_id"]
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.board_link.create",
                legacy_operation="spec.architecture.edit",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        link = await uow.services.design_systems.link_design_system_to_board(
            command.board_id,
            (command.payload or {})["design_system_id"],
            owner_id=actor.actor_id,
            board_access_authorized=True,
        )
        await commit(uow)
        return DataResult(link)


class UnlinkBoardDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        await _require_design_system_board(
            uow,
            command.board_id,
            actor,
            write=True,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.board_link.delete",
                legacy_operation="spec.architecture.edit",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        unlinked = await uow.services.design_systems.unlink_design_system_from_board(
            command.board_id
        )
        if unlinked:
            await commit(uow)
        return DataResult(unlinked)


class GetBoardDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        await _require_design_system_board(
            uow,
            command.board_id,
            actor,
            write=False,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.board_link.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        from okto_pulse.core.services.design_system import (
            project_effective_design_system,
        )

        effective = await uow.services.design_systems.get_board_effective_design_system(
            command.board_id
        )
        return DataResult(
            {
                "board_id": command.board_id,
                **project_effective_design_system(effective),
            }
        )


# --- screen mockups ---------------------------------------------------------


_ENTITY_TYPES = ("spec", "ideation", "refinement", "card", "story")


def _sanitize_html(html: str) -> str:
    sanitized = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    sanitized = re.sub(
        r"\s+on\w+\s*=\s*[\"'][^\"']*[\"']",
        "",
        sanitized,
        flags=re.IGNORECASE,
    )
    sanitized = re.sub(r"\s+on\w+\s*=\s*\S+", "", sanitized, flags=re.IGNORECASE)
    return sanitized


async def _load_mockup_entity(
    services: ApplicationServiceCatalog,
    entity_type: str,
    entity_id: str,
):
    dispatch = {
        "spec": (services.specs, "get_spec", "update_spec", ServicePayload),
        "ideation": (
            services.ideations,
            "get_ideation",
            "update_ideation",
            ServicePayload,
        ),
        "refinement": (
            services.refinements,
            "get_refinement",
            "update_refinement",
            ServicePayload,
        ),
        "card": (services.cards, "get_card", "update_card", ServicePayload),
        "story": (services.stories, "get_story", "update_story", ServicePayload),
    }
    service, get_name, update_name, update_class = dispatch[entity_type]
    entity = await getattr(service, get_name)(entity_id)
    return entity, service, update_name, update_class


def _validate_mockup_target(entity_type: str) -> None:
    if entity_type not in _ENTITY_TYPES:
        raise ScreenMockupUseCaseError(
            422,
            {
                "error": "invalid_entity_type",
                "code": "invalid_entity_type",
                "message": f"entity_type must be one of: {', '.join(_ENTITY_TYPES)}",
            },
        )
    if entity_type == "card":
        from okto_pulse.core.services import CARD_RESOURCE_READ_ONLY_MESSAGE

        raise ScreenMockupUseCaseError(409, CARD_RESOURCE_READ_ONLY_MESSAGE)


def _screen_entity_not_found(entity_type: str, entity_id: str) -> ScreenMockupUseCaseError:
    return ScreenMockupUseCaseError(
        404,
        {
            "error": "not_found",
            "message": f"{entity_type} '{entity_id}' not found",
        },
    )


async def _require_mockup_entity_write_access(
    uow: PulseUnitOfWork,
    *,
    entity: Any,
    entity_type: str,
    entity_id: str,
    actor: ActorContext,
) -> None:
    if entity is None or not await _has_board_access(
        uow,
        getattr(entity, "board_id", ""),
        actor,
        write=True,
    ):
        raise _screen_entity_not_found(entity_type, entity_id)
    if entity_type in {"ideation", "refinement", "spec"}:
        require_draft_mutation(entity, subject_type=entity_type)


@dataclass(frozen=True)
class CreateScreenMockupCommand:
    entity_type: str
    entity_id: str
    data: Any


@dataclass(frozen=True)
class UpdateScreenMockupCommand:
    entity_type: str
    entity_id: str
    screen_id: str
    data: Any


class CreateScreenMockupUseCase:
    async def execute(
        self, command: CreateScreenMockupCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            normalize_design_system_ref,
        )

        _validate_mockup_target(command.entity_type)
        entity, service, update_name, update_class = await _load_mockup_entity(
            uow.services, command.entity_type, command.entity_id
        )
        await _require_mockup_entity_write_access(
            uow,
            entity=entity,
            entity_type=command.entity_type,
            entity_id=command.entity_id,
            actor=actor,
        )

        screen = {
            "id": "sm_"
            + hashlib.md5(
                f"{command.entity_id}{command.data.title}{uuid.uuid4()}".encode()
            ).hexdigest()[:8],
            "title": command.data.title,
            "description": command.data.description,
            "screen_type": command.data.screen_type,
            "html_content": _sanitize_html(command.data.html_content),
            "annotations": [],
            "order": len(entity.screen_mockups or []),
            "design_system_ref": normalize_design_system_ref(
                command.data.design_system_ref,
                command.data.design_system_version,
            ),
            "design_system_evidence": command.data.design_system_evidence,
        }
        outcome = await uow.services.mockup_design_gate.evaluate_screen(
            entity.board_id,
            screen,
            entity_type=command.entity_type,
            entity_id=command.entity_id,
        )
        screens = list(entity.screen_mockups or []) + [screen]
        await getattr(service, update_name)(
            command.entity_id,
            actor.actor_id,
            update_class(screen_mockups=screens),
        )
        await commit(uow)
        return DataResult({"success": True, "screen": screen, "design_system_gate": outcome})


class UpdateScreenMockupUseCase:
    async def execute(
        self, command: UpdateScreenMockupCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            normalize_design_system_ref,
        )

        _validate_mockup_target(command.entity_type)
        entity, service, update_name, update_class = await _load_mockup_entity(
            uow.services, command.entity_type, command.entity_id
        )
        await _require_mockup_entity_write_access(
            uow,
            entity=entity,
            entity_type=command.entity_type,
            entity_id=command.entity_id,
            actor=actor,
        )

        screens = [dict(item) for item in (entity.screen_mockups or [])]
        screen = next((item for item in screens if item.get("id") == command.screen_id), None)
        if not screen:
            raise ScreenMockupUseCaseError(
                404,
                {"error": "not_found", "message": f"Screen '{command.screen_id}' not found"},
            )

        original = dict(screen)
        if command.data.title is not None:
            screen["title"] = command.data.title
        if command.data.description is not None:
            screen["description"] = command.data.description
        if command.data.screen_type is not None:
            screen["screen_type"] = command.data.screen_type
        if command.data.html_content is not None:
            screen["html_content"] = _sanitize_html(command.data.html_content)
        if command.data.design_system_ref is not None:
            screen["design_system_ref"] = normalize_design_system_ref(
                command.data.design_system_ref,
                command.data.design_system_version,
            )
        if command.data.design_system_evidence is not None:
            screen["design_system_evidence"] = command.data.design_system_evidence

        outcomes = await uow.services.mockup_design_gate.gate_delta(
            entity.board_id,
            [original],
            [screen],
            entity_type=command.entity_type,
            entity_id=command.entity_id,
        )
        await getattr(service, update_name)(
            command.entity_id,
            actor.actor_id,
            update_class(screen_mockups=screens),
        )
        await commit(uow)
        return DataResult(
            {
                "success": True,
                "screen": screen,
                "design_system_gate": outcomes[0]
                if outcomes
                else {"outcome": "not_applicable"},
            }
        )
