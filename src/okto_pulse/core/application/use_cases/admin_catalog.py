"""Admin/catalog REST use cases for AF35-S3 C3.

These use cases keep the REST handlers transport-only while preserving the
existing service/orchestrator behavior for amendment revisions, default board
configuration, design systems and screen mockups.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.application.use_cases.base import ActorContext, commit, session_of
from okto_pulse.core.application.use_cases._service_payload import ServicePayload


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


def _config_service(session: Any) -> Any:
    from okto_pulse.core.services.default_board_config_api import (
        DefaultBoardConfigApiService,
    )

    return DefaultBoardConfigApiService(session)


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


class CreateAmendmentRevisionUseCase:
    async def execute(
        self, command: CreateAmendmentRevisionCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.amendment_revision_api import (
            AmendmentRevisionApiService,
        )

        data = await AmendmentRevisionApiService(session_of(uow)).create(
            board_id=command.board_id,
            bug_id=command.bug_id,
            author=actor.actor_id,
            **command.payload,
        )
        await commit(uow)
        return DataResult(data)


class ListAmendmentRevisionsUseCase:
    async def execute(
        self, command: ListAmendmentRevisionsCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.amendment_revision_api import (
            AmendmentRevisionApiService,
        )

        return DataResult(
            await AmendmentRevisionApiService(session_of(uow)).list_for_bug(
                board_id=command.board_id, bug_id=command.bug_id
            )
        )


class GetAmendmentRevisionUseCase:
    async def execute(
        self, command: GetAmendmentRevisionCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.amendment_revision_api import (
            AmendmentRevisionApiService,
        )

        return DataResult(
            await AmendmentRevisionApiService(session_of(uow)).get(
                board_id=command.board_id,
                bug_id=command.bug_id,
                amendment_id=command.amendment_id,
            )
        )


class AssociateAmendmentRevisionUseCase:
    async def execute(
        self,
        command: AssociateAmendmentRevisionCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> DataResult:
        from okto_pulse.core.services.amendment_revision_api import (
            AmendmentRevisionApiService,
        )

        data = await AmendmentRevisionApiService(session_of(uow)).associate(
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
        uow: Any,
    ) -> DataResult:
        from okto_pulse.core.services.amendment_revision_api import (
            AmendmentRevisionApiService,
        )

        data = await AmendmentRevisionApiService(session_of(uow)).transition_lifecycle(
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
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        return DataResult(await _config_service(session_of(uow)).get_active(scope=command.scope))


class ListDefaultBoardConfigVersionsUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        return DataResult(await _config_service(session_of(uow)).list_versions(scope=command.scope))


class CreateDefaultBoardConfigVersionUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        data = await _config_service(session_of(uow)).create_version(
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
            **(command.payload or {}),
        )
        await commit(uow)
        return DataResult(data)


class ActivateDefaultBoardConfigVersionUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        data = await _config_service(session_of(uow)).activate_version(
            template_id=command.template_id,
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
        )
        await commit(uow)
        return DataResult(data)


class DeactivateDefaultBoardConfigVersionUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        data = await _config_service(session_of(uow)).deactivate_version(
            template_id=command.template_id,
            actor=actor.actor_id,
        )
        await commit(uow)
        return DataResult(data)


class GetBoardDefaultConfigDiffUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        return DataResult(
            await _config_service(session_of(uow)).get_board_diff(board_id=command.board_id)
        )


class ListDefaultGuidelineCandidatesUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        data = await _config_service(session_of(uow)).list_default_candidates(
            scope=command.scope,
            template_id=command.template_id or None,
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id or None),
        )
        return DataResult(data)


class UpdateDefaultGuidelineRefsUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        data = await _config_service(session_of(uow)).update_template_guidelines(
            template_id=command.template_id,
            guideline_default_refs=(command.payload or {}).get("guideline_default_refs"),
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
        )
        await commit(uow)
        return DataResult(data)


class SetDefaultDesignSystemUseCase:
    async def execute(
        self, command: DefaultBoardConfigCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        data = await _config_service(session_of(uow)).set_template_design_system(
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


class CreateDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
        )

        item = await DesignSystemService(session_of(uow)).create_design_system(
            actor.actor_id,
            **(command.payload or {}),
        )
        await commit(uow)
        return DataResult(serialize_design_system(item))


class ListDesignSystemsUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
        )

        items = await DesignSystemService(session_of(uow)).list_catalog(
            scope=command.scope,
            board_id=command.board_id or None,
        )
        return DataResult([serialize_design_system(item) for item in items])


class GetDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
        )

        item = await DesignSystemService(session_of(uow)).require_design_system(
            command.design_system_id
        )
        return DataResult(serialize_design_system(item))


class UpdateDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
        )

        item = await DesignSystemService(session_of(uow)).update_design_system(
            command.design_system_id,
            actor.actor_id,
            **(command.payload or {}),
        )
        await commit(uow)
        return DataResult(serialize_design_system(item))


class DeleteDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import DesignSystemService

        deleted = await DesignSystemService(session_of(uow)).delete_design_system(
            command.design_system_id,
            actor.actor_id,
        )
        if deleted:
            await commit(uow)
        return DataResult(deleted)


class LinkBoardDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import DesignSystemService

        link = await DesignSystemService(session_of(uow)).link_design_system_to_board(
            command.board_id,
            (command.payload or {})["design_system_id"],
        )
        await commit(uow)
        return DataResult(link)


class UnlinkBoardDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import DesignSystemService

        unlinked = await DesignSystemService(session_of(uow)).unlink_design_system_from_board(
            command.board_id
        )
        if unlinked:
            await commit(uow)
        return DataResult(unlinked)


class GetBoardDesignSystemUseCase:
    async def execute(
        self, command: DesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import DesignSystemService

        effective = await DesignSystemService(
            session_of(uow)
        ).get_board_effective_design_system(command.board_id)
        return DataResult({"board_id": command.board_id, "effective": effective})


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


async def _load_mockup_entity(session: Any, entity_type: str, entity_id: str):
    from okto_pulse.core.services import (
        CardService,
        IdeationService,
        RefinementService,
        SpecService,
        StoryService,
    )

    dispatch = {
        "spec": (SpecService, "get_spec", "update_spec", ServicePayload),
        "ideation": (IdeationService, "get_ideation", "update_ideation", ServicePayload),
        "refinement": (
            RefinementService,
            "get_refinement",
            "update_refinement",
            ServicePayload,
        ),
        "card": (CardService, "get_card", "update_card", ServicePayload),
        "story": (StoryService, "get_story", "update_story", ServicePayload),
    }
    service_cls, get_name, update_name, update_class = dispatch[entity_type]
    service = service_cls(session)
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
        self, command: CreateScreenMockupCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            MockupDesignSystemGate,
            normalize_design_system_ref,
        )

        _validate_mockup_target(command.entity_type)
        session = session_of(uow)
        entity, service, update_name, update_class = await _load_mockup_entity(
            session, command.entity_type, command.entity_id
        )
        if not entity:
            raise ScreenMockupUseCaseError(
                404,
                {
                    "error": "not_found",
                    "message": f"{command.entity_type} '{command.entity_id}' not found",
                },
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
        outcome = await MockupDesignSystemGate(session).evaluate_screen(
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
        self, command: UpdateScreenMockupCommand, *, actor: ActorContext, uow: Any
    ) -> DataResult:
        from okto_pulse.core.services.design_system import (
            MockupDesignSystemGate,
            normalize_design_system_ref,
        )

        _validate_mockup_target(command.entity_type)
        session = session_of(uow)
        entity, service, update_name, update_class = await _load_mockup_entity(
            session, command.entity_type, command.entity_id
        )
        if not entity:
            raise ScreenMockupUseCaseError(
                404,
                {
                    "error": "not_found",
                    "message": f"{command.entity_type} '{command.entity_id}' not found",
                },
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

        outcomes = await MockupDesignSystemGate(session).gate_delta(
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
