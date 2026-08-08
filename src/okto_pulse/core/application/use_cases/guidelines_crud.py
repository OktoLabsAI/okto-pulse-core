"""Guideline CRUD use cases (SaaS Refactor spec R01A REST-FU7-S3).

Transport-free reimplementations of every ``api/guidelines.py`` endpoint that
still bound ``get_db`` — the global guideline CRUD family (list / create / get /
update / delete) and the board guideline family (board listing, link-or-create
inline, unlink, priority update). Each use case delegates to the EXISTING
``GuidelineService`` / ``BoardService`` methods so the SQL inline, the version
bump, the governance metric and the link bookkeeping all stay in the service;
only the transport envelope (board-ownership 404, not-found, validate, mutate,
commit) moves here.

Behavioral fidelity to the legacy endpoints, with SK-B policy fences:

* Board ownership is enforced by ``_ensure_board`` (``BoardService.get_board``),
  raising ``EntityNotFoundError("board")`` — the adapter maps it to the legacy
  ``"Board not found"`` 404. Governed mutations additionally require the
  corresponding SK-B capability before any repository or service is touched.
* A missing-or-not-owned global guideline on update/delete is
  ``EntityNotFoundError("guideline_owned")`` → the legacy
  ``"Guideline not found or not owned by user"`` 404; a missing-or-not-owned
  guideline on ``get`` / on the link branch is ``EntityNotFoundError("guideline")``
  → ``"Guideline not found"``; a missing board↔guideline link on unlink / priority
  is ``EntityNotFoundError("link")`` → ``"Link not found"``.
* The inline-create branch's "must supply guideline_id OR title+content"
  validation raises ``CommandValidationError`` carrying the EXACT legacy 422
  detail string; the adapter maps it to ``422 UNPROCESSABLE_CONTENT``.
* Reads do not commit; writes ``commit(uow)`` after the service mutation,
  exactly as the legacy endpoints did.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    REVISIONS_CREATE,
    REVISIONS_RETIRE,
    require_policy_governance_capabilities,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyBindingConflict,
)
from okto_pulse.core.services.application_schemas import GuidelineCreate

# Exact legacy 422 detail for the inline-create validation branch.
_INLINE_CREATE_VALIDATION_DETAIL = (
    "Provide guideline_id to link a global guideline, or title and content to "
    "create an inline guideline."
)


def _query_scope_for_actor(
    actor: ActorContext, *, board_id: str | None = None
) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


def _actor_type(actor: ActorContext) -> str:
    return (
        "agent"
        if actor.source == "mcp"
        else "system"
        if actor.source == "system"
        else "user"
    )


async def _ensure_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool = False,
) -> QueryScope:
    """Resolve owner/share access and return a scope safe for downstream reads."""
    allowed_permissions = {"editor", "admin"} if write else None
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=allowed_permissions,
    )
    if not board:
        raise EntityNotFoundError("board", board_id)
    return ActorScope.from_context(actor).query_scope(
        target_board_id=board_id,
        allowed_board_ids={board_id},
        require_ownership=False,
    )


# ===========================================================================
# Global guidelines CRUD
# ===========================================================================


# --- list guidelines --------------------------------------------------------


class ListGuidelinesCommand:
    __slots__ = ("offset", "limit", "tag")

    def __init__(
        self, *, offset: int = 0, limit: int = 50, tag: str | None = None
    ) -> None:
        self.offset = offset
        self.limit = limit
        self.tag = tag


class ListGuidelinesResult:
    __slots__ = ("guidelines",)

    def __init__(self, guidelines: list[Any]) -> None:
        self.guidelines = guidelines


class ListGuidelinesUseCase:
    """List the actor's global guidelines (read, no commit). Filters to
    ``owner_id == actor`` + ``scope == 'global'`` (optionally by tag) via the
    service exactly as the legacy endpoint."""

    async def execute(
        self,
        command: ListGuidelinesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListGuidelinesResult:
        query_scope = _query_scope_for_actor(actor)
        guidelines = await uow.services.guidelines.list_guidelines(
            actor.actor_id,
            offset=command.offset,
            limit=command.limit,
            tag=command.tag,
            query_scope=query_scope,
        )
        return ListGuidelinesResult(guidelines)


# --- create guideline -------------------------------------------------------


class CreateGuidelineCommand:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class CreateGuidelineResult:
    __slots__ = ("guideline",)

    def __init__(self, guideline: Any) -> None:
        self.guideline = guideline


class CreateGuidelineUseCase:
    """Create a guideline owned by the actor (write). Delegates to
    ``create_guideline`` then commits, exactly as the legacy endpoint (no error
    mapping — the service raises nothing the legacy caught)."""

    async def execute(
        self,
        command: CreateGuidelineCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CreateGuidelineResult:
        require_policy_governance_capabilities(actor, REVISIONS_CREATE)
        board_id = getattr(command.data, "board_id", None)
        query_scope = _query_scope_for_actor(actor, board_id=board_id)
        if board_id:
            query_scope = await _ensure_board(uow, board_id, actor, write=True)
        guideline = await uow.services.guidelines.create_guideline(
            actor.actor_id,
            command.data,
            query_scope=query_scope,
            actor_type=_actor_type(actor),
        )
        await commit(uow)
        return CreateGuidelineResult(guideline)


# --- get guideline ----------------------------------------------------------


class GetGuidelineCommand:
    __slots__ = ("guideline_id",)

    def __init__(self, guideline_id: str) -> None:
        self.guideline_id = guideline_id


class GetGuidelineResult:
    __slots__ = ("guideline",)

    def __init__(self, guideline: Any) -> None:
        self.guideline = guideline


class GetGuidelineUseCase:
    """Get a guideline by id (read, no commit). A missing or not-owned guideline
    is ``EntityNotFoundError("guideline")`` (→ 404 "Guideline not found")."""

    async def execute(
        self, command: GetGuidelineCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetGuidelineResult:
        query_scope = _query_scope_for_actor(actor)
        guideline = await uow.services.guidelines.get_guideline(
            command.guideline_id,
            owner_id=actor.actor_id,
            query_scope=query_scope,
        )
        if not guideline:
            raise EntityNotFoundError("guideline", command.guideline_id)
        return GetGuidelineResult(guideline)


# --- update guideline -------------------------------------------------------


class UpdateGuidelineCommand:
    __slots__ = ("guideline_id", "data")

    def __init__(self, guideline_id: str, data: Any) -> None:
        self.guideline_id = guideline_id
        self.data = data


class UpdateGuidelineResult:
    __slots__ = ("guideline",)

    def __init__(self, guideline: Any) -> None:
        self.guideline = guideline


class UpdateGuidelineUseCase:
    """Update a guideline the actor owns (write). The service returns ``None`` for
    a missing OR not-owned guideline → ``EntityNotFoundError("guideline_owned")``
    (→ 404 "Guideline not found or not owned by user"); commits on success."""

    async def execute(
        self,
        command: UpdateGuidelineCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> UpdateGuidelineResult:
        require_policy_governance_capabilities(actor, REVISIONS_CREATE)
        query_scope = _query_scope_for_actor(actor)
        guideline = await uow.services.guidelines.update_guideline(
            command.guideline_id,
            actor.actor_id,
            command.data,
            query_scope=query_scope,
        )
        if not guideline:
            raise EntityNotFoundError("guideline_owned", command.guideline_id)
        await commit(uow)
        return UpdateGuidelineResult(guideline)


# --- delete guideline -------------------------------------------------------


class DeleteGuidelineCommand:
    __slots__ = ("guideline_id",)

    def __init__(self, guideline_id: str) -> None:
        self.guideline_id = guideline_id


class DeleteGuidelineResult:
    __slots__ = ()


class DeleteGuidelineUseCase:
    """Delete a guideline the actor owns (write). The service returns ``False`` for
    a missing OR not-owned guideline → ``EntityNotFoundError("guideline_owned")``
    (→ 404 "Guideline not found or not owned by user"); commits on success."""

    async def execute(
        self,
        command: DeleteGuidelineCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DeleteGuidelineResult:
        require_policy_governance_capabilities(actor, REVISIONS_RETIRE)
        query_scope = _query_scope_for_actor(actor)
        deleted = await uow.services.guidelines.delete_guideline(
            command.guideline_id,
            actor.actor_id,
            actor_type=(
                "agent"
                if actor.source == "mcp"
                else "system"
                if actor.source == "system"
                else "user"
            ),
            query_scope=query_scope,
        )
        if not deleted:
            raise EntityNotFoundError("guideline_owned", command.guideline_id)
        await commit(uow)
        return DeleteGuidelineResult()


# ===========================================================================
# Board guidelines (linked + inline)
# ===========================================================================


# --- list board guidelines --------------------------------------------------


class GetBoardGuidelinesCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class GetBoardGuidelinesResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class GetBoardGuidelinesUseCase:
    """List a board's guidelines — linked globals + inline, sorted by priority
    (read, no commit). Board ownership 404 before the service read; the service
    is called with ``surface="menu_board"`` exactly as the legacy endpoint."""

    async def execute(
        self,
        command: GetBoardGuidelinesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetBoardGuidelinesResult:
        query_scope = await _ensure_board(uow, command.board_id, actor)
        items = await uow.services.guidelines.get_board_guidelines(
            command.board_id,
            surface="menu_board",
            query_scope=query_scope,
        )
        return GetBoardGuidelinesResult(items)


# --- link or create board guideline -----------------------------------------


class LinkOrCreateBoardGuidelineCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class LinkOrCreateBoardGuidelineResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class LinkOrCreateBoardGuidelineUseCase:
    """Link an existing global guideline to a board OR create an inline board
    guideline (write, 201). Order mirrors the legacy endpoint exactly: board
    ownership 404 → ``guideline_id`` branch (link; a missing guideline is
    ``EntityNotFoundError("guideline")`` → 404 "Guideline not found") → otherwise
    the inline branch, which raises ``CommandValidationError`` (→ 422) with the
    EXACT legacy detail when title/content are missing, else creates an inline
    guideline. A single ``commit`` after the mutation. Returns the shaped payload
    dict (link or inline) for the adapter."""

    async def execute(
        self,
        command: LinkOrCreateBoardGuidelineCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> LinkOrCreateBoardGuidelineResult:
        require_policy_governance_capabilities(
            actor,
            ADOPTION_MANAGE if command.data.guideline_id else REVISIONS_CREATE,
        )
        query_scope = await _ensure_board(uow, command.board_id, actor, write=True)
        service = uow.services.guidelines
        data = command.data

        if data.guideline_id:
            raise GuidelinePolicyBindingConflict(
                "guideline_impact_preview_required"
            )

        if not data.title or not data.content:
            raise CommandValidationError(_INLINE_CREATE_VALIDATION_DETAIL)

        guideline = await service.create_guideline(
            actor.actor_id,
            GuidelineCreate(
                title=data.title,
                content=data.content,
                tags=data.tags,
                scope="inline",
                board_id=command.board_id,
                priority=data.priority,
            ),
            query_scope=query_scope,
            actor_type=_actor_type(actor),
        )
        await commit(uow)
        return LinkOrCreateBoardGuidelineResult(
            {
                "id": guideline.id,
                "board_id": command.board_id,
                "guideline_id": guideline.id,
                "priority": data.priority,
                "scope": "inline",
            }
        )


# --- unlink board guideline -------------------------------------------------


class UnlinkBoardGuidelineCommand:
    __slots__ = ("board_id", "guideline_id")

    def __init__(self, board_id: str, guideline_id: str) -> None:
        self.board_id = board_id
        self.guideline_id = guideline_id


class UnlinkBoardGuidelineResult:
    __slots__ = ()


class UnlinkBoardGuidelineUseCase:
    """Unlink a guideline from a board (write, 204). A missing link is
    ``EntityNotFoundError("link")`` (→ 404 "Link not found"); commits on success.
    Board access is checked first so cross-scope unlink attempts fail closed."""

    async def execute(
        self,
        command: UnlinkBoardGuidelineCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> UnlinkBoardGuidelineResult:
        require_policy_governance_capabilities(actor, ADOPTION_MANAGE)
        query_scope = await _ensure_board(uow, command.board_id, actor, write=True)
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
            raise EntityNotFoundError("link", command.guideline_id)
        await commit(uow)
        return UnlinkBoardGuidelineResult()


# --- update board guideline priority ----------------------------------------


class UpdateBoardGuidelinePriorityCommand:
    __slots__ = ("board_id", "guideline_id", "priority")

    def __init__(self, board_id: str, guideline_id: str, priority: int) -> None:
        self.board_id = board_id
        self.guideline_id = guideline_id
        self.priority = priority


class UpdateBoardGuidelinePriorityResult:
    __slots__ = ()


class UpdateBoardGuidelinePriorityUseCase:
    """Update the priority of a linked guideline on a board (write). A missing link
    is ``EntityNotFoundError("link")`` (→ 404 "Link not found"); commits on
    success. Board access is checked first so cross-scope priority updates fail
    closed. The adapter shapes the ``{board_id, guideline_id, priority}``
    response from its own path params + the requested priority."""

    async def execute(
        self,
        command: UpdateBoardGuidelinePriorityCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> UpdateBoardGuidelinePriorityResult:
        require_policy_governance_capabilities(actor, ADOPTION_MANAGE)
        query_scope = await _ensure_board(uow, command.board_id, actor, write=True)
        _ = query_scope
        raise GuidelinePolicyBindingConflict(
            "guideline_impact_preview_required"
        )
