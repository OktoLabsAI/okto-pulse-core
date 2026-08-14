"""MCP resource-gate and story use cases for AF35-S4.

The MCP server keeps authentication, coarse permission checks, parameter
coercion and envelope mapping. These use cases own the relational work through
the MCP UnitOfWork path so wrappers do not open ``get_db_for_mcp`` directly.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import require_authorization
from okto_pulse.core.application.use_cases.mutation_permissions import (
    entity_state,
    transition_permission_requirement,
)
from okto_pulse.core.domain.human_validation_cycle import require_draft_mutation
from okto_pulse.core.services.card_operational_freeze import (
    require_card_operational_mutation_allowed,
)


async def _require_resource_subject_draft(
    uow: PulseUnitOfWork,
    entity_type: str,
    entity_id: str,
    *,
    operation: str,
) -> None:
    if entity_type == "ideation":
        entity = await uow.services.ideations.get_ideation(entity_id)
    elif entity_type == "refinement":
        entity = await uow.services.refinements.get_refinement(entity_id)
    elif entity_type == "spec":
        entity = await uow.services.specs.get_spec(entity_id)
    elif entity_type == "card":
        entity = await uow.services.cards.get_card(entity_id)
    else:
        return
    if entity is None:
        raise ValueError(f"{entity_type}_not_found")
    if entity_type == "card":
        require_card_operational_mutation_allowed(entity, operation=operation)
    else:
        require_draft_mutation(entity, subject_type=entity_type)


@dataclass(frozen=True)
class McpPayloadResult:
    payload: Any


@dataclass(frozen=True)
class McpResourceGateSummaryCommand:
    board_id: str
    entity_type: str
    entity_id: str


class McpGetResourceGateSummaryUseCase:
    async def execute(
        self,
        command: McpResourceGateSummaryCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:

        summary = await uow.services.resource_gate.get_summary(
            command.board_id,
            command.entity_type,
            command.entity_id,
        )
        await commit(uow)
        return McpPayloadResult({"success": True, **summary})


@dataclass(frozen=True)
class McpMarkResourceNotApplicableCommand:
    board_id: str
    entity_type: str
    entity_id: str
    resource_type: str
    justification: str


class McpMarkResourceNotApplicableUseCase:
    async def execute(
        self,
        command: McpMarkResourceNotApplicableCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:

        await _require_resource_subject_draft(
            uow,
            command.entity_type,
            command.entity_id,
            operation="resource_gate.mark_not_applicable",
        )
        result = await uow.services.resource_gate.mark_not_applicable(
            command.board_id,
            command.entity_type,
            command.entity_id,
            command.resource_type,
            actor.actor_id,
            justification=command.justification,
            source_channel="mcp",
        )
        await commit(uow)
        return McpPayloadResult(result)


@dataclass(frozen=True)
class McpClearResourceNotApplicableCommand:
    board_id: str
    entity_type: str
    entity_id: str
    resource_type: str
    reason: str


class McpClearResourceNotApplicableUseCase:
    async def execute(
        self,
        command: McpClearResourceNotApplicableCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:

        await _require_resource_subject_draft(
            uow,
            command.entity_type,
            command.entity_id,
            operation="resource_gate.clear_not_applicable",
        )
        result = await uow.services.resource_gate.clear_not_applicable(
            command.board_id,
            command.entity_type,
            command.entity_id,
            command.resource_type,
            actor.actor_id,
            reason=command.reason or None,
        )
        await commit(uow)
        return McpPayloadResult(result)


def _mcp_story_state_perm(
    permissions: Any, granular: str | None, legacy: str | None, story: Any
) -> str | None:
    from okto_pulse.core.services.permission_policy import check_story_state_permission
    from okto_pulse.core.services.story_permissions import story_state

    return check_story_state_permission(
        permissions,
        granular or "",
        legacy,
        story,
        story_state=story_state(
            story.status, archived=bool(getattr(story, "archived", False))
        ),
    )


@dataclass(frozen=True)
class McpCreateStoryCommand:
    board_id: str
    data: Any


@dataclass(frozen=True)
class McpUpdateStoryCommand:
    board_id: str
    story_id: str
    update_data: dict[str, Any]
    data: Any


@dataclass(frozen=True)
class McpMoveStoryCommand:
    board_id: str
    story_id: str
    target_status: Any
    data: Any


@dataclass(frozen=True)
class McpArchiveStoryCommand:
    board_id: str
    story_id: str
    archived: bool


class McpStoryMutationResult:
    __slots__ = ("story", "not_found", "board_not_found", "perm_err")

    def __init__(
        self,
        *,
        story: Any = None,
        not_found: bool = False,
        board_not_found: bool = False,
        perm_err: str | None = None,
    ) -> None:
        self.story = story
        self.not_found = not_found
        self.board_not_found = board_not_found
        self.perm_err = perm_err


class McpCreateStoryUseCase:
    async def execute(
        self, command: McpCreateStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpStoryMutationResult:

        story = await uow.services.stories.create_story(
            command.board_id,
            actor.actor_id,
            command.data,
            skip_ownership_check=True,
        )
        await commit(uow)
        if not story:
            return McpStoryMutationResult(board_not_found=True)
        return McpStoryMutationResult(story=story)


class McpUpdateStoryUseCase:
    async def execute(
        self, command: McpUpdateStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpStoryMutationResult:
        from okto_pulse.core.services.permission_policy import Permissions
        from okto_pulse.core.services.story_permissions import story_update_permissions

        service = uow.services.stories
        existing = await service.get_story(command.story_id)
        if not existing or existing.board_id != command.board_id:
            return McpStoryMutationResult(not_found=True)
        for required_permission in story_update_permissions(command.update_data):
            perm_err = _mcp_story_state_perm(
                actor.permissions,
                required_permission,
                Permissions.SPECS_UPDATE,
                existing,
            )
            if perm_err:
                return McpStoryMutationResult(perm_err=perm_err)
        story = await service.update_story(command.story_id, actor.actor_id, command.data)
        await commit(uow)
        if not story or story.board_id != command.board_id:
            return McpStoryMutationResult(not_found=True)
        return McpStoryMutationResult(story=story)


class McpMoveStoryUseCase:
    async def execute(
        self, command: McpMoveStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpStoryMutationResult:
        service = uow.services.stories
        existing = await service.get_story(command.story_id)
        if (
            not existing
            or existing.board_id != command.board_id
            or actor.board_id != command.board_id
        ):
            return McpStoryMutationResult(not_found=True)
        if existing.archived:
            raise ValueError(
                "This story is archived. Restore it before changing status."
            )
        if entity_state(existing) == str(
            getattr(command.target_status, "value", command.target_status)
        ):
            return McpStoryMutationResult(story=existing)
        await require_authorization(
            actor,
            transition_permission_requirement(
                "story",
                existing.status,
                command.target_status,
                legacy_operation="specs:move",
            ),
            uow=uow,
            board_id=existing.board_id,
        )
        story = await service.move_story(command.story_id, actor.actor_id, command.data)
        await commit(uow)
        if not story or story.board_id != command.board_id:
            return McpStoryMutationResult(not_found=True)
        return McpStoryMutationResult(story=story)


class McpArchiveStoryUseCase:
    async def execute(
        self, command: McpArchiveStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpStoryMutationResult:
        from okto_pulse.core.services.permission_policy import Permissions

        service = uow.services.stories
        existing = await service.get_story(command.story_id)
        if not existing or existing.board_id != command.board_id:
            return McpStoryMutationResult(not_found=True)
        granular = "story.entity.archive" if command.archived else "story.entity.restore"
        perm_err = _mcp_story_state_perm(
            actor.permissions,
            granular,
            Permissions.SPECS_UPDATE,
            existing,
        )
        if perm_err:
            return McpStoryMutationResult(perm_err=perm_err)
        story = await service.archive_story(
            command.story_id, actor.actor_id, archived=command.archived
        )
        await commit(uow)
        if not story or story.board_id != command.board_id:
            return McpStoryMutationResult(not_found=True)
        return McpStoryMutationResult(story=story)
