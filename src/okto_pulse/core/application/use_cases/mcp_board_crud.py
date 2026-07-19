"""MCP-scoped board CRUD use cases (SaaS Refactor spec R01A MCP-FU6, family: board).

The board MCP tools have no board-scoping or atomic activity-log divergence (the
``board_id`` IS the scope), so these use cases are thinner than the card family's:
each wraps the existing service, transport-free, and the adapter keeps the exact
JSON / error envelope (per Codex's option-A-with-adapter-envelope decision).

The default-board-config tools (FR7, spec 9df814bc) have no REST use case to reuse
— their REST twin (api/default_board_config.py) calls the service directly — so
they get thin MCP use cases over ``DefaultBoardConfigApiService``.
``DefaultBoardConfigurationError`` propagates UNCAUGHT for the tool adapter's
legacy ``e.to_dict()`` envelope.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.scope import ActorScope, QueryScope


_MCP_HUMAN_ONLY_DEFAULT_BOARD_CONFIG_FIELDS = (
    "skip_task_requirement_link_gate_global",
)


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> QueryScope:
    actor_scope = ActorScope.from_context(actor)
    if board_id is None:
        return actor_scope.query_scope(target_board_id=board_id)
    return actor_scope.query_scope(
        target_board_id=board_id,
        allowed_board_ids=[board_id],
        require_ownership=False,
    )


# --- get_board (multi-service aggregation read) -----------------------------


class McpGetBoardCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpGetBoardResult:
    """Carries the board + its sibling collections + the effective design-system
    snapshot; the adapter shapes them into the legacy ``include``-aware payload
    (counts / design_system normalization / optional collections) INSIDE the UoW
    context so the lazy ``board.cards`` / ``board.settings`` load while live."""

    __slots__ = ("board", "agents", "specs", "ideations", "ds_effective_raw")

    def __init__(
        self,
        board: Any,
        agents: list[Any],
        specs: list[Any],
        ideations: list[Any],
        ds_effective_raw: Any,
    ) -> None:
        self.board = board
        self.agents = agents
        self.specs = specs
        self.ideations = ideations
        self.ds_effective_raw = ds_effective_raw


class McpGetBoardUseCase:
    """Fetch a board + its agents/specs/ideations + the effective design-system
    (read, no commit). A missing board is ``EntityNotFoundError`` → the adapter's
    legacy ``{"error": "Board not found"}``. ``board_id`` is the scope (no
    cross-board check). Transport-free: the ``include``-aware JSON shaping stays in
    the adapter."""

    async def execute(
        self, command: McpGetBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetBoardResult:

        board = await uow.services.boards.get_board(command.board_id)
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        agents = await uow.services.agents.list_agents_for_board(command.board_id)
        specs = await uow.services.specs.list_specs(command.board_id)
        ideations = await uow.services.ideations.list_ideations(command.board_id)
        ds_effective_raw = await uow.services.design_systems.get_board_effective_design_system(command.board_id)
        return McpGetBoardResult(board, agents, specs, ideations, ds_effective_raw)


class McpListBoardMembersCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpListBoardMembersResult:
    __slots__ = ("board", "agents")

    def __init__(self, board: Any, agents: list[Any]) -> None:
        self.board = board
        self.agents = agents


class McpListBoardMembersUseCase:
    """Fetch a board + its agents for the members listing (read, no commit) —
    the legacy two-call pattern (get_board + list_agents_for_board), NOT the full
    get_board aggregation. Missing board → ``EntityNotFoundError`` (adapter
    ``{"error": "Board not found"}``)."""

    async def execute(
        self, command: McpListBoardMembersCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpListBoardMembersResult:

        board = await uow.services.boards.get_board(command.board_id)
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        agents = await uow.services.agents.list_agents_for_board(command.board_id)
        return McpListBoardMembersResult(board, agents)


# --- default board config: reads --------------------------------------------


class McpGetActiveDefaultBoardConfigCommand:
    __slots__ = ("scope",)

    def __init__(self, scope: str) -> None:
        self.scope = scope


class _DataResult:
    """Shared thin carrier — the adapter ``json.dumps(result.data, default=str)``."""

    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class McpGetActiveDefaultBoardConfigUseCase:
    """Active default board-config template for a scope (read, no commit)."""

    async def execute(
        self, command: McpGetActiveDefaultBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        data = await uow.services.default_board_config.get_active(scope=command.scope)
        return _DataResult(data)


class McpListDefaultBoardConfigVersionsCommand:
    __slots__ = ("scope",)

    def __init__(self, scope: str) -> None:
        self.scope = scope


class McpListDefaultBoardConfigVersionsUseCase:
    """List default board-config template versions for a scope (read, no commit)."""

    async def execute(
        self,
        command: McpListDefaultBoardConfigVersionsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        data = await uow.services.default_board_config.list_versions(scope=command.scope)
        return _DataResult(data)


class McpGetBoardDefaultConfigDiffCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpGetBoardDefaultConfigDiffUseCase:
    """Field-level diff between a board's applied template snapshot and its current
    settings (read, no commit)."""

    async def execute(
        self,
        command: McpGetBoardDefaultConfigDiffCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        data = await uow.services.default_board_config.get_board_diff(
            board_id=command.board_id
        )
        return _DataResult(data)


# --- default board config: writes (commit) ----------------------------------


class McpCreateDefaultBoardConfigVersionCommand:
    __slots__ = (
        "settings_payload", "scope", "guideline_default_refs",
        "design_system_default_ref", "activate",
    )

    def __init__(
        self,
        *,
        settings_payload: Any,
        scope: str,
        guideline_default_refs: Any,
        design_system_default_ref: Any,
        activate: bool,
    ) -> None:
        self.settings_payload = settings_payload
        self.scope = scope
        self.guideline_default_refs = guideline_default_refs
        self.design_system_default_ref = design_system_default_ref
        self.activate = activate


class McpCreateDefaultBoardConfigVersionUseCase:
    """Create a new default board-config template version (write). The single-active
    invariant + BoardSettings validation stay in the service; commit via UoW."""

    async def execute(
        self,
        command: McpCreateDefaultBoardConfigVersionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        service = uow.services.default_board_config
        query_scope = _query_scope_for_actor(actor)
        settings_payload = await _preserve_mcp_human_only_default_settings(
            service, command.settings_payload, command.scope
        )
        data = await service.create_version(
            actor=actor.actor_id,
            settings_payload=settings_payload,
            scope=command.scope,
            guideline_default_refs=command.guideline_default_refs,
            design_system_default_ref=command.design_system_default_ref,
            activate=command.activate,
            query_scope=query_scope,
        )
        await commit(uow)
        return _DataResult(data)


async def _preserve_mcp_human_only_default_settings(
    service: Any,
    settings_payload: Any,
    scope: str,
) -> Any:
    """MCP cannot alter human-only skips, but omitted fields must not clear them."""
    if not isinstance(settings_payload, dict):
        return settings_payload
    active_result = await service.get_active(scope=scope)
    active = active_result.get("active") or {}
    active_settings = active.get("settings_payload") or {}
    if not isinstance(active_settings, dict):
        return settings_payload

    preserved = dict(settings_payload)
    for field in _MCP_HUMAN_ONLY_DEFAULT_BOARD_CONFIG_FIELDS:
        if field not in preserved and field in active_settings:
            preserved[field] = active_settings[field]
    return preserved


class McpActivateDefaultBoardConfigVersionCommand:
    __slots__ = ("template_id",)

    def __init__(self, template_id: str) -> None:
        self.template_id = template_id


class McpActivateDefaultBoardConfigVersionUseCase:
    """Activate a template version (write); the service deactivates every other
    active version in the scope. Commit via UoW."""

    async def execute(
        self,
        command: McpActivateDefaultBoardConfigVersionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        query_scope = _query_scope_for_actor(actor)
        data = await uow.services.default_board_config.activate_version(
            template_id=command.template_id,
            actor=actor.actor_id,
            query_scope=query_scope,
        )
        await commit(uow)
        return _DataResult(data)


class McpDeactivateDefaultBoardConfigVersionCommand:
    __slots__ = ("template_id",)

    def __init__(self, template_id: str) -> None:
        self.template_id = template_id


class McpDeactivateDefaultBoardConfigVersionUseCase:
    """Deactivate a template version (write). Commit via UoW."""

    async def execute(
        self,
        command: McpDeactivateDefaultBoardConfigVersionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        data = await uow.services.default_board_config.deactivate_version(
            template_id=command.template_id, actor=actor.actor_id
        )
        await commit(uow)
        return _DataResult(data)


# --- board ↔ guideline links (GuidelineService) -----------------------------


class McpGetBoardGuidelinesCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpGetBoardGuidelinesUseCase:
    """Board guidelines merged + sorted for the MCP surface (read, no commit)."""

    async def execute(
        self, command: McpGetBoardGuidelinesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:

        query_scope = _query_scope_for_actor(actor, board_id=command.board_id)
        items = await uow.services.guidelines.get_board_guidelines(
            command.board_id,
            surface="mcp",
            owner_id=actor.actor_id,
            query_scope=query_scope,
        )
        return _DataResult(items)


class McpLinkGuidelineToBoardCommand:
    __slots__ = ("board_id", "guideline_id", "priority")

    def __init__(self, board_id: str, guideline_id: str, priority: int) -> None:
        self.board_id = board_id
        self.guideline_id = guideline_id
        self.priority = priority


class McpLinkGuidelineToBoardUseCase:
    """Link a global guideline to a board (write). A missing guideline is
    ``EntityNotFoundError("guideline", ...)`` → adapter ``"Guideline not found"``.
    Returns the link (the adapter reads ``.priority``)."""

    async def execute(
        self, command: McpLinkGuidelineToBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:

        service = uow.services.guidelines
        query_scope = _query_scope_for_actor(actor, board_id=command.board_id)
        guideline = await service.get_guideline(
            command.guideline_id,
            owner_id=actor.actor_id,
            query_scope=query_scope,
        )
        if not guideline:
            raise EntityNotFoundError("guideline", command.guideline_id)
        link = await service.link_guideline_to_board(
            command.board_id,
            command.guideline_id,
            command.priority,
            owner_id=actor.actor_id,
            query_scope=query_scope,
        )
        if not link:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return _DataResult(link)


class McpUnlinkGuidelineFromBoardCommand:
    __slots__ = ("board_id", "guideline_id")

    def __init__(self, board_id: str, guideline_id: str) -> None:
        self.board_id = board_id
        self.guideline_id = guideline_id


class McpUnlinkGuidelineFromBoardUseCase:
    """Unlink a guideline from a board (write). No matching link is
    ``EntityNotFoundError("guideline_link", ...)`` → adapter ``"Link not found"``."""

    async def execute(
        self, command: McpUnlinkGuidelineFromBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:

        query_scope = _query_scope_for_actor(actor, board_id=command.board_id)
        unlinked = await uow.services.guidelines.unlink_guideline_from_board(
            command.board_id,
            command.guideline_id,
            owner_id=actor.actor_id,
            query_scope=query_scope,
        )
        if not unlinked:
            raise EntityNotFoundError("guideline_link", command.guideline_id)
        await commit(uow)
        return _DataResult(unlinked)


class McpUpdateBoardGuidelinePriorityCommand:
    __slots__ = ("board_id", "guideline_id", "priority")

    def __init__(self, board_id: str, guideline_id: str, priority: int) -> None:
        self.board_id = board_id
        self.guideline_id = guideline_id
        self.priority = priority


class McpUpdateBoardGuidelinePriorityUseCase:
    """Update a board guideline through an MCP-authorized board grant."""

    async def execute(
        self,
        command: McpUpdateBoardGuidelinePriorityCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        query_scope = _query_scope_for_actor(actor, board_id=command.board_id)
        updated = await uow.services.guidelines.update_priority(
            command.board_id,
            command.guideline_id,
            command.priority,
            owner_id=actor.actor_id,
            query_scope=query_scope,
        )
        if not updated:
            raise EntityNotFoundError("guideline_link", command.guideline_id)
        await commit(uow)
        return _DataResult(updated)


# --- board ↔ design-system links (DesignSystemService) ----------------------


async def _require_design_system_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
) -> None:
    from okto_pulse.core.services.design_system import DesignSystemError

    if await load_accessible_board(uow, board_id, actor) is None:
        raise DesignSystemError(
            "board_not_found",
            f"Board '{board_id}' not found.",
            404,
            {"board_id": board_id},
        )


class McpLinkBoardDesignSystemCommand:
    __slots__ = ("board_id", "design_system_id")

    def __init__(self, board_id: str, design_system_id: str) -> None:
        self.board_id = board_id
        self.design_system_id = design_system_id


class McpLinkBoardDesignSystemUseCase:
    """Set the board's single effective Design System (write). ``DesignSystemError``
    propagates UNCAUGHT for the adapter's ``e.to_dict()``. Returns the link (the
    adapter reads ``.board_id``/``.design_system_id``/``.design_system_version``)."""

    async def execute(
        self, command: McpLinkBoardDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        await _require_design_system_board(uow, command.board_id, actor)
        link = await uow.services.design_systems.link_design_system_to_board(
            command.board_id,
            command.design_system_id,
            owner_id=actor.actor_id,
            board_access_authorized=True,
        )
        await commit(uow)
        return _DataResult(link)


class McpUnlinkBoardDesignSystemCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpUnlinkBoardDesignSystemUseCase:
    """Remove the board's effective Design System link (write). ``DesignSystemError``
    propagates UNCAUGHT for the adapter. Returns the ``unlinked`` flag."""

    async def execute(
        self, command: McpUnlinkBoardDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        await _require_design_system_board(uow, command.board_id, actor)
        unlinked = await uow.services.design_systems.unlink_design_system_from_board(
            command.board_id
        )
        await commit(uow)
        return _DataResult(unlinked)


class McpGetBoardDesignSystemCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpGetBoardDesignSystemUseCase:
    """Resolve the board's effective Design System from persisted state (read, no
    commit). ``DesignSystemError`` propagates UNCAUGHT for the adapter."""

    async def execute(
        self, command: McpGetBoardDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        await _require_design_system_board(uow, command.board_id, actor)
        effective = await uow.services.design_systems.get_board_effective_design_system(
            command.board_id
        )
        return _DataResult(effective)


# --- list_by_board (entity_type fetch dispatcher + pure-data post-filters) ---


def _apply_label_filter(items: list[Any], filters: dict) -> list[Any]:
    """Pure-data label post-filter, identical to the legacy per-branch logic."""
    raw = filters.get("labels")
    if not raw:
        return items
    label_filter = raw if isinstance(raw, list) else [raw]
    return [
        it
        for it in items
        if any(lbl in (getattr(it, "labels", None) or []) for lbl in label_filter)
    ]


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _optional_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


def is_derivation_pending_ideation(item: Any) -> bool:
    status = _enum_value(getattr(item, "status", None))
    complexity = _enum_value(getattr(item, "complexity", None))
    if status != "done":
        return False
    if complexity in {"medium", "large"}:
        active_refinements = int(getattr(item, "active_refinement_count", 0) or 0)
        return active_refinements == 0
    if complexity == "small":
        active_specs = int(getattr(item, "active_spec_count", 0) or 0)
        return active_specs == 0
    return False


def is_derivation_pending_refinement(item: Any) -> bool:
    status = _enum_value(getattr(item, "status", None))
    active_count = int(getattr(item, "active_spec_count", 0) or 0)
    return status == "done" and active_count == 0


def _apply_derivation_pending_filter(
    items: list[Any],
    filters: dict,
    predicate: Any,
) -> list[Any]:
    expected = _optional_bool(filters.get("derivation_pending"))
    if expected is None:
        return items
    return [item for item in items if predicate(item) is expected]


class McpListByBoardCommand:
    __slots__ = ("board_id", "entity_type", "filters", "story_args", "topic_args")

    def __init__(
        self,
        board_id: str,
        entity_type: str,
        filters: dict,
        *,
        story_args: dict | None = None,
        topic_args: dict | None = None,
    ) -> None:
        self.board_id = board_id
        self.entity_type = entity_type
        self.filters = filters
        self.story_args = story_args or {}
        self.topic_args = topic_args or {}


class McpListByBoardUseCase:
    """Fetch a board's top-level entities by ``entity_type`` + apply the pure-data
    post-filters (labels/assignee/status), read-only. Transport-free: the adapter
    owns ``filters`` JSON parsing/``validate_filters``, the required-filter checks
    (``refinement``→``ideation_id``, ``sprint``→``spec_id``, validated BEFORE this
    call), the story/topic bool-arg computation (server helpers), pagination and
    per-type JSON shaping. ``story_args``/``topic_args`` carry the adapter's
    pre-computed kwargs so the server helpers stay out of the core."""

    async def execute(
        self, command: McpListByBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:

        et = command.entity_type
        f = command.filters

        # The transport authenticates the requested board, but this use case is
        # also a reusable application boundary.  Reject a spoofed command before
        # resolving any parent identifier so an actor cannot use a board-scoped
        # list as an existence oracle for another board.
        if actor.board_id != command.board_id:
            return _DataResult([])

        if et == "spec":
            items = await uow.services.specs.list_specs(command.board_id, f.get("status"))
            items = _apply_label_filter(items, f)
            if f.get("assignee_id"):
                items = [s for s in items if s.assignee_id == f["assignee_id"]]
        elif et == "ideation":
            items = await uow.services.ideations.list_ideations(
                command.board_id, f.get("status")
            )
            items = _apply_label_filter(items, f)
            items = _apply_derivation_pending_filter(
                items,
                f,
                is_derivation_pending_ideation,
            )
        elif et == "refinement":
            ideation_id = f.get("ideation_id", "")
            ideation = await uow.services.ideations.get_ideation(ideation_id)
            if not ideation or ideation.board_id != command.board_id:
                return _DataResult([])
            items = await uow.services.refinements.list_refinements(ideation_id)
            # Contain corrupt legacy rows as well as cross-board parent probes.
            items = [r for r in items if r.board_id == command.board_id]
            if f.get("status"):
                items = [r for r in items if r.status.value == f["status"]]
            items = _apply_label_filter(items, f)
            items = _apply_derivation_pending_filter(
                items,
                f,
                is_derivation_pending_refinement,
            )
        elif et == "sprint":
            spec_id = f.get("spec_id", "")
            spec = await uow.services.specs.get_spec(spec_id)
            if not spec or spec.board_id != command.board_id:
                return _DataResult([])
            items = await uow.services.sprints.list_sprints(spec_id)
            # A legacy database can contain relationally valid but cross-board
            # children; never project those through the authenticated board.
            items = [s for s in items if s.board_id == command.board_id]
            if f.get("status"):
                items = [s for s in items if s.status.value == f["status"]]
        elif et == "story":
            items = await uow.services.stories.list_stories(
                command.board_id, **command.story_args
            )
        else:  # topic
            items = await uow.services.stories.list_topics(
                command.board_id, **command.topic_args
            )
        return _DataResult(items)
