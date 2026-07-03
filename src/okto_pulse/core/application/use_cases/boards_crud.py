"""Board CRUD + shares/archive use cases (SaaS Refactor spec R01A REST-FU7-S1).

Transport-free reimplementations of the ``api/boards.py`` endpoints that still
bound a raw request session — list / get / update / delete board, create-card,
columns, archive/restore tree, and the board-share CRUD. ``create_board`` and
the agent-board override write already live in their own modules
(``create_board.py`` / ``update_board_overrides.py``) and are NOT duplicated
here.

Each use case delegates to the EXISTING ``BoardService`` / ``CardService`` /
``ShareService`` / ``ArchiveService`` / ``AgentService`` method — the SQL inline
in those services stays in the service; only the transport envelope (lookup →
not-found / not-authorized → mutate → commit / re-fetch / shape) moves here.
Reads do NOT commit; writes ``commit(uow)`` after the service mutation, exactly
as the legacy endpoints did.

Error mapping (the adapter reproduces the legacy status + detail off the typed
error): a missing board / share-membership is ``EntityNotFoundError`` (→ 404); a
``ShareService`` write returning ``None``/``False`` (owner/admin gate) is
``PermissionDeniedError`` (→ 403) carrying the legacy detail string; the
``CardOperationError`` (→ 409 ``to_dict``) and ``ValueError`` (create-card → 400,
archive/restore → 400) raised by the services propagate uncaught for the adapter
to map. The board effective-settings normalization (``BoardGovernanceService``)
is applied inside the read/update use cases exactly as ``create_board`` does.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
    session_of,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.services import (
    AgentService,
    BoardService,
    CardService,
    ShareService,
)
from okto_pulse.core.services.board_governance import BoardGovernanceService


def _attach_effective_board_settings(board: Any) -> Any:
    """Normalize a board's persisted settings to the effective gate config —
    the transport-free twin of ``api/boards.py:_attach_effective_board_settings``."""
    if board is not None:
        board.__dict__["settings"] = BoardGovernanceService.normalize_settings(
            getattr(board, "settings", None)
        )
    return board


def _query_scope_for_actor(
    actor: ActorContext,
    *,
    board_id: str | None = None,
    require_ownership: bool = True,
) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(
        target_board_id=board_id,
        require_ownership=require_ownership,
    )


# --- list -------------------------------------------------------------------


class ListBoardsCommand:
    __slots__ = ("offset", "limit", "view")

    def __init__(self, *, offset: int = 0, limit: int = 20, view: str = "my") -> None:
        self.offset = offset
        self.limit = limit
        self.view = view


class ListBoardsResult:
    __slots__ = ("boards",)

    def __init__(self, boards: list[Any]) -> None:
        self.boards = boards


class ListBoardsUseCase:
    """List the boards visible to the actor (read, no commit). Delegates to
    ``BoardService.list_boards`` (the ``my``/``shared``/``all`` SQL stays there),
    discards the total (the legacy endpoint did), and attaches effective settings
    to each board — exactly as the legacy endpoint did."""

    async def execute(
        self, command: ListBoardsCommand, *, actor: ActorContext, uow: Any
    ) -> ListBoardsResult:
        boards, _ = await BoardService(session_of(uow)).list_boards(
            actor.actor_id,
            command.offset,
            command.limit,
            view=command.view,
            query_scope=_query_scope_for_actor(actor),
        )
        for board in boards:
            _attach_effective_board_settings(board)
        return ListBoardsResult(boards)


# --- get --------------------------------------------------------------------


class GetBoardCommand:
    __slots__ = ("board_id", "compact")

    def __init__(self, board_id: str, *, compact: bool = False) -> None:
        self.board_id = board_id
        self.compact = compact


class GetBoardResult:
    __slots__ = ("board",)

    def __init__(self, board: Any) -> None:
        self.board = board


class GetBoardUseCase:
    """Fetch a board with its agents + card/agent counts (read, no commit).
    ``EntityNotFoundError("board")`` when missing/not owned (adapter → 404 "Board
    not found"). When ``compact`` the inline cards/agents are zeroed out (counts
    preserved) — exactly as the legacy endpoint did."""

    async def execute(
        self, command: GetBoardCommand, *, actor: ActorContext, uow: Any
    ) -> GetBoardResult:
        session = session_of(uow)
        board = await BoardService(session).get_board(
            command.board_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        board_agents = await AgentService(session).list_agents_for_board(command.board_id)
        cards_list = list(board.cards or [])
        board.__dict__["counts"] = {
            "cards": len(cards_list),
            "agents": len(board_agents),
        }
        if command.compact:
            board.__dict__["agents"] = []
            board.__dict__["cards"] = []
        else:
            board.__dict__["agents"] = board_agents
        return GetBoardResult(_attach_effective_board_settings(board))


# --- update -----------------------------------------------------------------


class UpdateBoardCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class UpdateBoardResult:
    __slots__ = ("board",)

    def __init__(self, board: Any) -> None:
        self.board = board


class UpdateBoardUseCase:
    """Update a board (write). ``EntityNotFoundError("board")`` when missing/not
    owned (adapter → 404 "Board not found"). Commits, then re-fetches via
    ``get_board`` so the result carries the board with relationships + agents
    loaded and effective settings attached — exactly as the legacy endpoint did."""

    async def execute(
        self, command: UpdateBoardCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateBoardResult:
        session = session_of(uow)
        service = BoardService(session)
        board = await service.update_board(
            command.board_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        board = await service.get_board(
            command.board_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        board.__dict__["agents"] = await AgentService(session).list_agents_for_board(
            command.board_id
        )
        return UpdateBoardResult(_attach_effective_board_settings(board))


# --- delete -----------------------------------------------------------------


class DeleteBoardCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class DeleteBoardResult:
    __slots__ = ()


class DeleteBoardUseCase:
    """Delete a board and all its cards (write). ``EntityNotFoundError("board")``
    when missing/not owned (adapter → 404 "Board not found"); commits after the
    delete."""

    async def execute(
        self, command: DeleteBoardCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteBoardResult:
        deleted = await BoardService(session_of(uow)).delete_board(
            command.board_id, actor.actor_id
        )
        if not deleted:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return DeleteBoardResult()


# --- create card in board ---------------------------------------------------


class CreateCardInBoardCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class CreateCardInBoardResult:
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class CreateCardInBoardUseCase:
    """Create a card in a board (write). Delegates to ``CardService.create_card``;
    its ``CardOperationError`` (→ 409 ``to_dict``) and ``ValueError`` (→ 400)
    propagate uncaught for the adapter to map in that exact order. A ``None``
    return (board missing/not owned) becomes ``EntityNotFoundError("board")``
    (adapter → 404 "Board not found or not owned by user"). Commits, then
    re-fetches via ``get_card`` so the result carries the card with relationships
    loaded — exactly as the legacy endpoint did."""

    async def execute(
        self, command: CreateCardInBoardCommand, *, actor: ActorContext, uow: Any
    ) -> CreateCardInBoardResult:
        service = CardService(session_of(uow))
        card = await service.create_card(
            command.board_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not card:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return CreateCardInBoardResult(await service.get_card(card.id))


# --- board columns (read) ---------------------------------------------------


class GetBoardColumnsCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class GetBoardColumnsResult:
    __slots__ = ("board",)

    def __init__(self, board: Any) -> None:
        self.board = board


class GetBoardColumnsUseCase:
    """Fetch a board so the adapter can group its cards by status/column (read, no
    commit). ``EntityNotFoundError("board")`` when missing/not owned (adapter →
    404 "Board not found"). The cards/qa_items are eager-loaded by
    ``BoardService.get_board``; the per-card projection + ``include_archived``
    filtering stay in the adapter, exactly as the legacy endpoint shaped them."""

    async def execute(
        self, command: GetBoardColumnsCommand, *, actor: ActorContext, uow: Any
    ) -> GetBoardColumnsResult:
        board = await BoardService(session_of(uow)).get_board(
            command.board_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        return GetBoardColumnsResult(board)


# --- archive / restore tree -------------------------------------------------


class ArchiveTreeCommand:
    __slots__ = ("entity_type", "entity_id")

    def __init__(self, entity_type: str, entity_id: str) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id


class ArchiveTreeResult:
    __slots__ = ("counts",)

    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts


class ArchiveTreeUseCase:
    """Archive an entity and all its descendants in cascade (write). Delegates to
    ``ArchiveService.archive_tree`` (the cascade + SQL stay there); its
    ``ValueError`` (unknown entity_type → 400) propagates uncaught for the adapter
    to map. Commits after the cascade — exactly as the legacy endpoint did."""

    async def execute(
        self, command: ArchiveTreeCommand, *, actor: ActorContext, uow: Any
    ) -> ArchiveTreeResult:
        from okto_pulse.core.services.main import ArchiveService

        counts = await ArchiveService(session_of(uow)).archive_tree(
            command.entity_type, command.entity_id
        )
        await commit(uow)
        return ArchiveTreeResult(counts)


class RestoreTreeCommand:
    __slots__ = ("entity_type", "entity_id")

    def __init__(self, entity_type: str, entity_id: str) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id


class RestoreTreeResult:
    __slots__ = ("counts",)

    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts


class RestoreTreeUseCase:
    """Restore an archived entity and all its descendants (write). Delegates to
    ``ArchiveService.restore_tree``; its ``ValueError`` (→ 400) propagates uncaught
    for the adapter to map. Commits after the cascade — exactly as the legacy
    endpoint did."""

    async def execute(
        self, command: RestoreTreeCommand, *, actor: ActorContext, uow: Any
    ) -> RestoreTreeResult:
        from okto_pulse.core.services.main import ArchiveService

        counts = await ArchiveService(session_of(uow)).restore_tree(
            command.entity_type, command.entity_id
        )
        await commit(uow)
        return RestoreTreeResult(counts)


# --- board shares -----------------------------------------------------------


class ShareBoardCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class ShareBoardResult:
    __slots__ = ("share",)

    def __init__(self, share: Any) -> None:
        self.share = share


class ShareBoardUseCase:
    """Share a board with another user, owner/admin only (write). Delegates to
    ``ShareService.share_board`` (the manage-shares gate + self-share guard stay
    there) with the actor's ``realm_id`` (falling back to ``""`` exactly as the
    legacy endpoint). A ``None`` return becomes ``PermissionDeniedError`` carrying
    the legacy 403 detail; commits after the share."""

    async def execute(
        self, command: ShareBoardCommand, *, actor: ActorContext, uow: Any
    ) -> ShareBoardResult:
        share = await ShareService(session_of(uow)).share_board(
            command.board_id,
            actor.actor_id,
            actor.realm_id or "",
            command.data,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not share:
            raise PermissionDeniedError(
                "Not authorized to share this board or invalid target user"
            )
        await commit(uow)
        return ShareBoardResult(share)


class ListBoardSharesCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class ListBoardSharesResult:
    __slots__ = ("shares",)

    def __init__(self, shares: list[Any]) -> None:
        self.shares = shares


class ListBoardSharesUseCase:
    """List a board's shares (read, no commit). Verifies the caller has access via
    ``ShareService.get_user_permission`` first — no permission becomes
    ``EntityNotFoundError("board")`` (adapter → 404 "Board not found"), exactly as
    the legacy endpoint did."""

    async def execute(
        self, command: ListBoardSharesCommand, *, actor: ActorContext, uow: Any
    ) -> ListBoardSharesResult:
        service = ShareService(session_of(uow))
        perm = await service.get_user_permission(
            command.board_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not perm:
            raise EntityNotFoundError("board", command.board_id)
        return ListBoardSharesResult(await service.list_shares(command.board_id))


class UpdateBoardShareCommand:
    __slots__ = ("share_id", "data")

    def __init__(self, share_id: str, data: Any) -> None:
        self.share_id = share_id
        self.data = data


class UpdateBoardShareResult:
    __slots__ = ("share",)

    def __init__(self, share: Any) -> None:
        self.share = share


class UpdateBoardShareUseCase:
    """Update a share's permission, owner/admin only (write). Delegates to
    ``ShareService.update_share`` (the manage-shares gate stays there). A ``None``
    return (missing share OR not authorized) becomes ``PermissionDeniedError``
    carrying the legacy 403 detail; commits after the update."""

    async def execute(
        self, command: UpdateBoardShareCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateBoardShareResult:
        share = await ShareService(session_of(uow)).update_share(
            command.share_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(actor),
        )
        if not share:
            raise PermissionDeniedError("Not authorized to update this share")
        await commit(uow)
        return UpdateBoardShareResult(share)


class RevokeBoardShareCommand:
    __slots__ = ("share_id",)

    def __init__(self, share_id: str) -> None:
        self.share_id = share_id


class RevokeBoardShareResult:
    __slots__ = ()


class RevokeBoardShareUseCase:
    """Revoke a share — owner/admin can revoke, the shared user can leave (write).
    Delegates to ``ShareService.revoke_share`` (the authorization stays there). A
    ``False`` return becomes ``PermissionDeniedError`` carrying the legacy 403
    detail; commits after the revoke."""

    async def execute(
        self, command: RevokeBoardShareCommand, *, actor: ActorContext, uow: Any
    ) -> RevokeBoardShareResult:
        revoked = await ShareService(session_of(uow)).revoke_share(
            command.share_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
        )
        if not revoked:
            raise PermissionDeniedError("Not authorized to revoke this share")
        await commit(uow)
        return RevokeBoardShareResult()
