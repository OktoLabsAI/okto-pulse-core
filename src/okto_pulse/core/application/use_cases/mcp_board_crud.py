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


from okto_pulse.core.domain.enums import CardStatus, RefinementStatus, SpecStatus
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    GroupCountRequest,
    PageRequest,
    PageResult,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    require_policy_governance_capabilities,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyBindingConflict,
)
from okto_pulse.core.services.default_board_configuration import (
    guideline_ref_diff_has_changes,
)


_MCP_HUMAN_ONLY_DEFAULT_BOARD_CONFIG_FIELDS = (
    "skip_task_requirement_link_gate_global",
)


def _query_scope_for_actor(
    actor: ActorContext, *, board_id: str | None = None
) -> QueryScope:
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
        ds_effective_raw = (
            await uow.services.design_systems.get_board_effective_design_system(
                command.board_id
            )
        )
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
        self,
        command: McpListBoardMembersCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpListBoardMembersResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "board.share.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
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
        self,
        command: McpGetActiveDefaultBoardConfigCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.read",
                legacy_operation="board.read",
            ),
            uow=uow,
        )
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
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.read",
                legacy_operation="board.read",
            ),
            uow=uow,
        )
        data = await uow.services.default_board_config.list_versions(
            scope=command.scope
        )
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
        return _DataResult(data)


# --- default board config: writes (commit) ----------------------------------


class McpCreateDefaultBoardConfigVersionCommand:
    __slots__ = (
        "settings_payload",
        "scope",
        "guideline_default_refs",
        "design_system_default_ref",
        "activate",
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
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.create",
                legacy_operation="spec.entity.edit_fields",
            ),
            uow=uow,
        )
        query_scope = _query_scope_for_actor(actor)
        settings_payload = await _preserve_mcp_human_only_default_settings(
            service, command.settings_payload, command.scope
        )
        diff = await service.preview_create_guideline_ref_diff(
            scope=command.scope,
            guideline_default_refs=command.guideline_default_refs,
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
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.activate",
                legacy_operation="spec.entity.edit_fields",
            ),
            uow=uow,
        )
        query_scope = _query_scope_for_actor(actor)
        diff = (
            await uow.services.default_board_config.preview_activate_guideline_ref_diff(
                template_id=command.template_id,
            )
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
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.deactivate",
                legacy_operation="spec.entity.edit_fields",
            ),
            uow=uow,
        )
        diff = (
            await uow.services.default_board_config.preview_deactivate_guideline_ref_diff(
                template_id=command.template_id,
            )
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
        self,
        command: McpGetBoardGuidelinesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
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
        self,
        command: McpLinkGuidelineToBoardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:

        require_policy_governance_capabilities(actor, ADOPTION_MANAGE)
        raise GuidelinePolicyBindingConflict(
            "guideline_impact_preview_required"
        )


class McpUnlinkGuidelineFromBoardCommand:
    __slots__ = ("board_id", "guideline_id")

    def __init__(self, board_id: str, guideline_id: str) -> None:
        self.board_id = board_id
        self.guideline_id = guideline_id


class McpUnlinkGuidelineFromBoardUseCase:
    """Unlink a guideline from a board (write). No matching link is
    ``EntityNotFoundError("guideline_link", ...)`` → adapter ``"Link not found"``."""

    async def execute(
        self,
        command: McpUnlinkGuidelineFromBoardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:

        require_policy_governance_capabilities(actor, ADOPTION_MANAGE)
        query_scope = _query_scope_for_actor(actor, board_id=command.board_id)
        unlinked = await uow.services.guidelines.unlink_guideline_from_board(
            command.board_id,
            command.guideline_id,
            actor_type=(
                "agent"
                if actor.source == "mcp"
                else "system"
                if actor.source == "system"
                else "user"
            ),
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
        require_policy_governance_capabilities(actor, ADOPTION_MANAGE)
        raise GuidelinePolicyBindingConflict(
            "guideline_impact_preview_required"
        )


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
        self,
        command: McpLinkBoardDesignSystemCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        await _require_design_system_board(uow, command.board_id, actor)
        await uow.services.design_systems.require_design_system(command.design_system_id)
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
        self,
        command: McpUnlinkBoardDesignSystemCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        await _require_design_system_board(uow, command.board_id, actor)
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
        self,
        command: McpGetBoardDesignSystemCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        await _require_design_system_board(uow, command.board_id, actor)
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
        return _DataResult(project_effective_design_system(effective))


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


def _strict_filter_bool(
    value: Any,
    *,
    field: str,
    default: bool = False,
) -> bool:
    """Parse the documented bool compatibility literals without truthiness.

    The MCP adapter normally normalizes these values first, but this application
    boundary is reusable and must not turn ``"false"``/``"0"`` into ``True``.
    """

    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise ValueError(f"{field} must be a boolean or one of true/false, 1/0, yes/no")


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


def _page_scope(
    board_id: str, *, include_archived: bool = False, **parents: str
) -> tuple[ApplicationFilter, ...]:
    scope = [ApplicationFilter("board_id", "eq", board_id)]
    scope.extend(
        ApplicationFilter(field, "eq", value) for field, value in parents.items()
    )
    if not include_archived:
        scope.append(ApplicationFilter("archived", "is_false"))
    return tuple(scope)


def _label_groups(raw: Any) -> tuple[tuple[ApplicationFilter, ...], ...]:
    if not raw:
        return ()
    labels = raw if isinstance(raw, list) else [raw]
    return tuple(
        (
            ApplicationFilter(
                "labels",
                "json_member",
                label,
            ),
        )
        for label in labels
    )


def _empty_page(offset: int, limit: int) -> PageResult:
    return PageResult((), 0, 0, offset, limit)


class McpListCardsByStatusCommand:
    __slots__ = (
        "board_id",
        "status",
        "spec_id",
        "priority",
        "assignee_id",
        "include_archived",
        "offset",
        "limit",
    )

    def __init__(
        self,
        board_id: str,
        *,
        status: str = "",
        spec_id: str = "",
        priority: str = "",
        assignee_id: str = "",
        include_archived: bool = False,
        offset: int = 0,
        limit: int = 50,
    ) -> None:
        self.board_id = board_id
        self.status = status
        self.spec_id = spec_id
        self.priority = priority
        self.assignee_id = assignee_id
        self.include_archived = include_archived
        self.offset = offset
        self.limit = limit


class McpListCardsByStatusUseCase:
    """MCP card-list contract backed by the catalogued SQL page executor."""

    async def execute(
        self,
        command: McpListCardsByStatusCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise EntityNotFoundError("board", command.board_id)

        filters: list[ApplicationFilter] = []
        if command.status == "open":
            filters.append(
                ApplicationFilter(
                    "status",
                    "in",
                    tuple(
                        item.value
                        for item in CardStatus
                        if item not in {CardStatus.DONE, CardStatus.CANCELLED}
                    ),
                )
            )
        elif command.status:
            filters.append(ApplicationFilter("status", "eq", command.status))
        if command.spec_id:
            filters.append(ApplicationFilter("spec_id", "eq", command.spec_id))
        if command.priority:
            filters.append(ApplicationFilter("priority", "eq", command.priority))
        if command.assignee_id:
            filters.append(ApplicationFilter("assignee_id", "eq", command.assignee_id))

        page = await uow.services.entity_pages.list(
            PageRequest(
                surface="mcp_card_status_list",
                scope=_page_scope(
                    command.board_id,
                    include_archived=command.include_archived,
                ),
                filters=tuple(filters),
                offset=command.offset,
                limit=command.limit,
            )
        )
        return _DataResult(page)


class McpListByBoardCommand:
    __slots__ = (
        "board_id",
        "entity_type",
        "filters",
        "story_args",
        "topic_args",
        "offset",
        "limit",
    )

    def __init__(
        self,
        board_id: str,
        entity_type: str,
        filters: dict,
        *,
        story_args: dict | None = None,
        topic_args: dict | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> None:
        self.board_id = board_id
        self.entity_type = entity_type
        self.filters = filters
        self.story_args = story_args or {}
        self.topic_args = topic_args or {}
        self.offset = offset
        self.limit = limit


class McpListByBoardUseCase:
    """Execute every supported MCP board list through one typed SQL page.

    The inbound adapter still owns filter-payload validation and per-type JSON
    shaping.  Core owns scope, predicates, canonical per-tool order, both
    counts, the SQL window and page-local derived counters.
    """

    async def execute(
        self,
        command: McpListByBoardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:

        et = command.entity_type
        f = command.filters

        # The transport authenticates the requested board, but this use case is
        # also a reusable application boundary.  Reject a spoofed command before
        # resolving any parent identifier so an actor cannot use a board-scoped
        # list as an existence oracle for another board.
        if actor.board_id != command.board_id:
            return _DataResult(_empty_page(command.offset, command.limit))

        filters: list[ApplicationFilter] = []
        any_groups: tuple[tuple[ApplicationFilter, ...], ...] = ()
        includes: tuple[str, ...] = ()

        if et == "spec":
            surface = "mcp_spec_list"
            scope = _page_scope(
                command.board_id,
                include_archived=_strict_filter_bool(
                    f.get("include_archived"),
                    field="include_archived",
                ),
            )
            if f.get("status"):
                filters.append(ApplicationFilter("status", "eq", f["status"]))
            if f.get("assignee_id"):
                filters.append(ApplicationFilter("assignee_id", "eq", f["assignee_id"]))
            any_groups = _label_groups(f.get("labels"))
        elif et == "ideation":
            surface = "mcp_ideation_list"
            scope = _page_scope(
                command.board_id,
                include_archived=_strict_filter_bool(
                    f.get("include_archived"),
                    field="include_archived",
                ),
            )
            if f.get("status"):
                filters.append(ApplicationFilter("status", "eq", f["status"]))
            pending = _optional_bool(f.get("derivation_pending"))
            if pending is not None:
                filters.append(
                    ApplicationFilter(
                        "derivation_pending",
                        "is_true" if pending else "is_false",
                    )
                )
            any_groups = _label_groups(f.get("labels"))
        elif et == "refinement":
            ideation_id = f.get("ideation_id", "")
            parent = await uow.ideations.get(ideation_id)
            if parent is None or parent.board_id != command.board_id:
                return _DataResult(_empty_page(command.offset, command.limit))
            surface = "mcp_refinement_list"
            scope = _page_scope(
                command.board_id,
                include_archived=_strict_filter_bool(
                    f.get("include_archived"),
                    field="include_archived",
                ),
                ideation_id=ideation_id,
            )
            if f.get("status"):
                filters.append(ApplicationFilter("status", "eq", f["status"]))
            pending = _optional_bool(f.get("derivation_pending"))
            if pending is not None:
                filters.append(
                    ApplicationFilter(
                        "derivation_pending",
                        "is_true" if pending else "is_false",
                    )
                )
            any_groups = _label_groups(f.get("labels"))
        elif et == "sprint":
            spec_id = f.get("spec_id", "")
            parent = await uow.specs.get(spec_id)
            if parent is None or parent.board_id != command.board_id:
                return _DataResult(_empty_page(command.offset, command.limit))
            surface = "mcp_sprint_list"
            scope = _page_scope(
                command.board_id,
                include_archived=_strict_filter_bool(
                    f.get("include_archived"),
                    field="include_archived",
                ),
                spec_id=spec_id,
            )
            if f.get("status"):
                filters.append(ApplicationFilter("status", "eq", f["status"]))
        elif et == "story":
            surface = "mcp_story_list"
            args = command.story_args
            scope = _page_scope(
                command.board_id,
                include_archived=_strict_filter_bool(
                    args.get("include_archived"),
                    field="include_archived",
                ),
            )
            if args.get("status_filter"):
                filters.append(ApplicationFilter("status", "eq", args["status_filter"]))
            if args.get("topic_id"):
                filters.append(ApplicationFilter("topic_id", "eq", args["topic_id"]))
            for field in ("linked", "converted"):
                value = args.get(field)
                if value is not None:
                    filters.append(
                        ApplicationFilter(
                            field,
                            "is_true" if value else "is_false",
                        )
                    )
        else:  # topic
            surface = "mcp_topic_list"
            scope = _page_scope(
                command.board_id,
                include_archived=_strict_filter_bool(
                    command.topic_args.get("include_archived"),
                    field="include_archived",
                ),
            )

        page = await uow.services.entity_pages.list(
            PageRequest(
                surface=surface,
                scope=scope,
                filters=tuple(filters),
                any_groups=any_groups,
                includes=includes,
                offset=command.offset,
                limit=command.limit,
            )
        )
        await _attach_mcp_page_derivatives(
            uow,
            command.board_id,
            et,
            page.items,
        )
        return _DataResult(page)


async def _attach_mcp_page_derivatives(
    uow: PulseUnitOfWork,
    board_id: str,
    entity_type: str,
    items: tuple[Any, ...],
) -> None:
    """Attach legacy-derived fields with page-bounded aggregate queries."""
    if not items:
        return

    ids = tuple(item.id for item in items)
    if entity_type == "ideation":
        refinement_rows = await uow.services.entity_pages.group_count(
            GroupCountRequest(
                surface="mcp_ideation_refinement_counts",
                scope=(
                    ApplicationFilter("board_id", "eq", board_id),
                    ApplicationFilter("archived", "is_false"),
                ),
                filters=(
                    ApplicationFilter("ideation_id", "in", ids),
                    ApplicationFilter(
                        "status",
                        "in",
                        tuple(
                            status.value
                            for status in RefinementStatus
                            if status != RefinementStatus.CANCELLED
                        ),
                    ),
                ),
                group_by=("ideation_id",),
            )
        )
        spec_rows = await uow.services.entity_pages.group_count(
            GroupCountRequest(
                surface="mcp_spec_child_counts",
                scope=(
                    ApplicationFilter("board_id", "eq", board_id),
                    ApplicationFilter("archived", "is_false"),
                ),
                filters=(
                    ApplicationFilter("ideation_id", "in", ids),
                    ApplicationFilter("refinement_id", "is_none"),
                    ApplicationFilter(
                        "status",
                        "in",
                        tuple(
                            status.value
                            for status in SpecStatus
                            if status != SpecStatus.CANCELLED
                        ),
                    ),
                ),
                group_by=("ideation_id",),
            )
        )
        refinement_counts = {row.values[0]: row.count for row in refinement_rows}
        spec_counts = {row.values[0]: row.count for row in spec_rows}
        for item in items:
            item.attach("active_refinement_count", refinement_counts.get(item.id, 0))
            item.attach("active_spec_count", spec_counts.get(item.id, 0))
    elif entity_type == "refinement":
        rows = await uow.services.entity_pages.group_count(
            GroupCountRequest(
                surface="mcp_spec_child_counts",
                scope=(
                    ApplicationFilter("board_id", "eq", board_id),
                    ApplicationFilter("archived", "is_false"),
                ),
                filters=(
                    ApplicationFilter("refinement_id", "in", ids),
                    ApplicationFilter(
                        "status",
                        "in",
                        tuple(
                            status.value
                            for status in SpecStatus
                            if status != SpecStatus.CANCELLED
                        ),
                    ),
                ),
                group_by=("refinement_id",),
            )
        )
        counts = {row.values[0]: row.count for row in rows}
        for item in items:
            item.attach("active_spec_count", counts.get(item.id, 0))
    elif entity_type == "sprint":
        for item in items:
            item.attach(
                "normal_sprint_created", _enum_value(item.lane_type) == "normal"
            )
    elif entity_type == "topic":
        rows = await uow.services.entity_pages.group_count(
            GroupCountRequest(
                surface="topic_story_counts",
                scope=(ApplicationFilter("board_id", "eq", board_id),),
                group_by=("topic_id", "archived"),
            )
        )
        counts: dict[str, dict[str, int]] = {}
        for row in rows:
            topic_id, archived = row.values
            if topic_id is None:
                continue
            bucket = counts.setdefault(
                topic_id, {"active_count": 0, "archived_count": 0}
            )
            key = "archived_count" if bool(archived) else "active_count"
            bucket[key] += row.count
        for item in items:
            bucket = counts.get(item.id, {"active_count": 0, "archived_count": 0})
            item.attach("story_count", bucket["active_count"])
            item.attach("active_count", bucket["active_count"])
            item.attach("archived_count", bucket["archived_count"])
            item.attach(
                "total_associated_count",
                bucket["active_count"] + bucket["archived_count"],
            )
