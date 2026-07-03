"""Guideline CRUD use cases (SaaS Refactor spec R01A REST-FU7-S3).

Transport-free reimplementations of every ``api/guidelines.py`` endpoint that
still bound ``get_db`` — the global guideline CRUD family (list / create / get /
update / delete) and the board guideline family (board listing, link-or-create
inline, unlink, priority update). Each use case delegates to the EXISTING
``GuidelineService`` / ``BoardService`` methods so the SQL inline, the version
bump, the governance metric and the link bookkeeping all stay in the service;
only the transport envelope (board-ownership 404, not-found, validate, mutate,
commit) moves here.

Behavioral fidelity to the legacy endpoints:

* Board ownership is enforced by ``_ensure_board`` (``BoardService.get_board``),
  raising ``EntityNotFoundError("board")`` — the adapter maps it to the legacy
  ``"Board not found"`` 404. This is the ONLY access gate the legacy guideline
  endpoints applied (there is no ``_require_permissions`` here), so no extra
  permission resolution is introduced.
* A missing-or-not-owned global guideline on update/delete is
  ``EntityNotFoundError("guideline_owned")`` → the legacy
  ``"Guideline not found or not owned by user"`` 404; a missing guideline on
  ``get`` / on the link branch is ``EntityNotFoundError("guideline")`` →
  ``"Guideline not found"``; a missing board↔guideline link on unlink / priority
  is ``EntityNotFoundError("link")`` → ``"Link not found"``.
* The inline-create branch's "must supply guideline_id OR title+content"
  validation raises ``CommandValidationError`` carrying the EXACT legacy 422
  detail string; the adapter maps it to ``422 UNPROCESSABLE_CONTENT``.
* Reads do not commit; writes ``commit(uow)`` after the service mutation,
  exactly as the legacy endpoints did.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.services.application_schemas import GuidelineCreate
from okto_pulse.core.services import BoardService, GuidelineService

# Exact legacy 422 detail for the inline-create validation branch.
_INLINE_CREATE_VALIDATION_DETAIL = (
    "Provide guideline_id to link a global guideline, or title and content to "
    "create an inline guideline."
)


async def _ensure_board(session: Any, board_id: str, user_id: str) -> None:
    """Reproduce the legacy board guard: raise ``EntityNotFoundError("board")``
    (adapter → 404 "Board not found") when the board is missing / not owned."""
    board = await BoardService(session).get_board(board_id, user_id)
    if not board:
        raise EntityNotFoundError("board", board_id)


# ===========================================================================
# Global guidelines CRUD
# ===========================================================================


# --- list guidelines --------------------------------------------------------


class ListGuidelinesCommand:
    __slots__ = ("offset", "limit", "tag")

    def __init__(self, *, offset: int = 0, limit: int = 50, tag: str | None = None) -> None:
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
        self, command: ListGuidelinesCommand, *, actor: ActorContext, uow: Any
    ) -> ListGuidelinesResult:
        session = session_of(uow)
        guidelines = await GuidelineService(session).list_guidelines(
            actor.actor_id, offset=command.offset, limit=command.limit, tag=command.tag
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
        self, command: CreateGuidelineCommand, *, actor: ActorContext, uow: Any
    ) -> CreateGuidelineResult:
        session = session_of(uow)
        guideline = await GuidelineService(session).create_guideline(
            actor.actor_id, command.data
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
    """Get a guideline by id (read, no commit). A missing guideline is
    ``EntityNotFoundError("guideline")`` (→ 404 "Guideline not found"). No
    ownership check — mirrors the legacy endpoint exactly."""

    async def execute(
        self, command: GetGuidelineCommand, *, actor: ActorContext, uow: Any
    ) -> GetGuidelineResult:
        session = session_of(uow)
        guideline = await GuidelineService(session).get_guideline(command.guideline_id)
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
        self, command: UpdateGuidelineCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateGuidelineResult:
        session = session_of(uow)
        guideline = await GuidelineService(session).update_guideline(
            command.guideline_id, actor.actor_id, command.data
        )
        if not guideline:
            raise EntityNotFoundError("guideline_owned", command.guideline_id)
        await commit(uow)
        # Re-load the columns inside the async context so the server-side
        # ``updated_at`` (server_onupdate) is materialized as a plain value before
        # the adapter serializes it — otherwise the commit-expired attribute
        # lazy-loads outside the greenlet (MissingGreenlet) on the UoW path.
        await session.refresh(guideline)
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
        self, command: DeleteGuidelineCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteGuidelineResult:
        session = session_of(uow)
        deleted = await GuidelineService(session).delete_guideline(
            command.guideline_id, actor.actor_id
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
        self, command: GetBoardGuidelinesCommand, *, actor: ActorContext, uow: Any
    ) -> GetBoardGuidelinesResult:
        session = session_of(uow)
        await _ensure_board(session, command.board_id, actor.actor_id)
        items = await GuidelineService(session).get_board_guidelines(
            command.board_id, surface="menu_board"
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
        self, command: LinkOrCreateBoardGuidelineCommand, *, actor: ActorContext, uow: Any
    ) -> LinkOrCreateBoardGuidelineResult:
        session = session_of(uow)
        await _ensure_board(session, command.board_id, actor.actor_id)
        service = GuidelineService(session)
        data = command.data

        if data.guideline_id:
            guideline = await service.get_guideline(data.guideline_id)
            if not guideline:
                raise EntityNotFoundError("guideline", data.guideline_id)
            link = await service.link_guideline_to_board(
                command.board_id, data.guideline_id, data.priority
            )
            await commit(uow)
            return LinkOrCreateBoardGuidelineResult(
                {
                    "id": link.id,
                    "board_id": command.board_id,
                    "guideline_id": data.guideline_id,
                    "priority": link.priority,
                }
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
            ),
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
    Mirrors the legacy endpoint (no board ownership pre-check, exactly as
    legacy)."""

    async def execute(
        self, command: UnlinkBoardGuidelineCommand, *, actor: ActorContext, uow: Any
    ) -> UnlinkBoardGuidelineResult:
        session = session_of(uow)
        unlinked = await GuidelineService(session).unlink_guideline_from_board(
            command.board_id, command.guideline_id
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
    success. The adapter shapes the legacy ``{board_id, guideline_id, priority}``
    response from its own path params + the requested priority, exactly as
    legacy."""

    async def execute(
        self,
        command: UpdateBoardGuidelinePriorityCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> UpdateBoardGuidelinePriorityResult:
        session = session_of(uow)
        updated = await GuidelineService(session).update_priority(
            command.board_id, command.guideline_id, command.priority
        )
        if not updated:
            raise EntityNotFoundError("link", command.guideline_id)
        await commit(uow)
        return UpdateBoardGuidelinePriorityResult()
