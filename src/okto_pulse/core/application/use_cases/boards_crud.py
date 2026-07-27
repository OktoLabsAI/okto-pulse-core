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

import asyncio
import logging

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.services.board_governance import BoardGovernanceService

logger = logging.getLogger(__name__)


def _attach_value(entity: Any, name: str, value: Any) -> None:
    attach = getattr(entity, "attach", None)
    if callable(attach):
        attach(name, value)
    else:
        entity.__dict__[name] = value


def _attach_effective_board_settings(board: Any) -> Any:
    """Normalize a board's persisted settings to the effective gate config —
    the transport-free twin of ``api/boards.py:_attach_effective_board_settings``."""
    if board is not None:
        _attach_value(
            board,
            "settings",
            BoardGovernanceService.normalize_settings(getattr(board, "settings", None)),
        )
    return board


def _query_scope_for_actor(
    actor: ActorContext,
    *,
    board_id: str | None = None,
    require_ownership: bool = True,
    allowed_board_ids: set[str] | None = None,
) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(
        target_board_id=board_id,
        allowed_board_ids=allowed_board_ids,
        require_ownership=require_ownership,
    )


async def _load_readable_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
) -> Any | None:
    """Load a board only after owner/share access has been established.

    ``BoardService.get_board`` intentionally defaults to owner-only queries.  A
    verified share therefore receives an explicit one-board allowlist rather
    than weakening the service's default ownership contract globally.
    """

    if await load_accessible_board(uow, board_id, actor) is None:
        return None
    return await uow.services.boards.get_board(
        board_id,
        actor.actor_id,
        query_scope=_query_scope_for_actor(
            actor,
            board_id=board_id,
            require_ownership=False,
            allowed_board_ids={board_id},
        ),
    )


async def _require_owned_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
) -> Any:
    """Preflight an owner-only REST board mutation.

    Missing, cross-realm and merely shared boards deliberately collapse to the
    same not-found error before a mutating service is called.
    """

    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=(),
    )
    if board is None or getattr(board, "owner_id", None) != actor.actor_id:
        raise EntityNotFoundError("board", board_id)
    return board


async def _require_readable_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
) -> Any:
    """Preflight a share operation without exposing inaccessible boards."""

    board = await load_accessible_board(uow, board_id, actor)
    if board is None:
        raise EntityNotFoundError("board", board_id)
    return board


async def _require_share_on_board(
    uow: PulseUnitOfWork,
    board_id: str,
    share_id: str,
) -> Any:
    """Resolve a share only inside its already-authorized parent board."""

    for share in await uow.services.shares.list_shares(board_id):
        if getattr(share, "id", None) == share_id:
            return share
    raise EntityNotFoundError("share", share_id)


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
        self, command: ListBoardsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListBoardsResult:
        boards, _ = await uow.services.boards.list_boards(
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
        self, command: GetBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetBoardResult:
        board = await _load_readable_board(uow, command.board_id, actor)
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        board_agents = await uow.services.agents.list_agents_for_board(command.board_id)
        cards_list = list(board.cards or [])
        _attach_value(
            board,
            "counts",
            {"cards": len(cards_list), "agents": len(board_agents)},
        )
        if command.compact:
            _attach_value(board, "agents", [])
            _attach_value(board, "cards", [])
        else:
            _attach_value(board, "agents", board_agents)
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
        self, command: UpdateBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateBoardResult:
        await _require_owned_board(uow, command.board_id, actor)
        service = uow.services.boards
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
        _attach_value(
            board,
            "agents",
            await uow.services.agents.list_agents_for_board(command.board_id),
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
    """Delete a board, its cards, and every board-scoped KG projection.

    The source delete is flushed under an administrative KG writer lease, then
    strict KG erasure is verified. Relational KG cleanup and the board delete
    share one UoW commit while the lease remains held; any KG failure prevents
    that commit.
    """

    async def execute(
        self, command: DeleteBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteBoardResult:
        pending_job = await uow.services.kg.get_board_erasure_job(command.board_id)
        if pending_job is None:
            await _require_owned_board(uow, command.board_id, actor)
        elif getattr(pending_job, "actor_id", None) != actor.actor_id:
            # A continuation must not turn an erased board identifier into an
            # existence oracle for another actor.
            raise EntityNotFoundError("board", command.board_id)

        async def _terminal_erasure() -> None:
            async with uow.services.kg.board_erasure_scope(
                command.board_id,
                actor_id=actor.actor_id,
            ) as erasure:
                current_job = await uow.services.kg.get_board_erasure_job(
                    command.board_id
                )
                if current_job is not None:
                    if getattr(current_job, "actor_id", None) != actor.actor_id:
                        raise EntityNotFoundError("board", command.board_id)
                else:
                    deleted = await uow.services.boards.delete_board(
                        command.board_id, actor.actor_id
                    )
                    if not deleted:
                        raise EntityNotFoundError("board", command.board_id)
                    await uow.synchronize()
                    await uow.services.kg.stage_board_relational_erasure(
                        command.board_id,
                        actor_id=actor.actor_id,
                    )
                    erasure.ensure_owned()
                    # Source deletion, relational KG cleanup and the durable
                    # continuation are atomic.
                    await commit(uow)
                    erasure.ensure_owned()

                # External stores are touched only after the source Board and
                # relational KG/KB rows are durably gone. The idempotent
                # physical phase remains under both board and global fences.
                # If it fails, the committed continuation is retried by the
                # dedicated runtime worker without requiring the Board row.
                try:
                    await uow.services.kg.right_to_erasure(
                        command.board_id,
                        strict=True,
                        commit=False,
                        global_writer_guarded=True,
                        purge_relational=False,
                    )
                    erasure.ensure_owned()
                    completed = await uow.services.kg.complete_board_erasure_job(
                        command.board_id
                    )
                    if not completed:
                        raise RuntimeError(
                            "board_erasure_continuation_missing "
                            f"board={command.board_id}"
                        )
                    await commit(uow)
                except Exception as exc:
                    try:
                        await uow.services.kg.record_board_erasure_failure(
                            command.board_id,
                            exc,
                        )
                        await commit(uow)
                    except Exception:
                        # The initial transaction already made the continuation
                        # durable. A failed diagnostic update must not mask the
                        # physical-erasure error or destroy the retry handle.
                        logger.exception(
                            "board_erasure.failure_record_failed board=%s",
                            command.board_id,
                        )
                    try:
                        from okto_pulse.core.application.runtime_workers import (
                            signal_runtime_worker,
                        )

                        signal_runtime_worker("board_erasure_worker")
                    except Exception:
                        logger.exception(
                            "board_erasure.worker_signal_failed board=%s",
                            command.board_id,
                        )
                    raise

        terminal = asyncio.create_task(
            _terminal_erasure(),
            name=f"board-erasure:{command.board_id}",
        )
        try:
            await asyncio.shield(terminal)
        except asyncio.CancelledError:
            # Once the source delete starts, request cancellation must not
            # strand a half-erased board. Drain the terminal saga while its UoW
            # and writer fences are still alive, then preserve cancellation for
            # the caller.
            while not terminal.done():
                try:
                    await asyncio.shield(terminal)
                except asyncio.CancelledError:
                    continue
            if not terminal.cancelled() and terminal.exception() is not None:
                logger.error(
                    "board_erasure.failed_after_request_cancellation board=%s error=%r",
                    command.board_id,
                    terminal.exception(),
                )
            raise
        return DeleteBoardResult()


# --- create card in board ---------------------------------------------------


class CreateCardInBoardCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class CreateCardInBoardResult:
    __slots__ = ("card", "knowledge_mutation")

    def __init__(self, card: Any, knowledge_mutation: Any = None) -> None:
        self.card = card
        self.knowledge_mutation = knowledge_mutation


class CreateCardInBoardUseCase:
    """Create a card in a board (write). Delegates to ``CardService.create_card``;
    its ``CardOperationError`` (→ 409 ``to_dict``) and ``ValueError`` (→ 400)
    propagate uncaught for the adapter to map in that exact order. A ``None``
    return (board missing/not owned) becomes ``EntityNotFoundError("board")``
    (adapter → 404 "Board not found or not owned by user"). Commits, then
    re-fetches via ``get_card`` so the result carries the card with relationships
    loaded — exactly as the legacy endpoint did."""

    async def execute(
        self,
        command: CreateCardInBoardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CreateCardInBoardResult:
        await _require_owned_board(uow, command.board_id, actor)
        if getattr(command.data, "knowledge_propagation", None) is not None:
            from okto_pulse.core.application.use_cases.knowledge_propagation import (
                CreateCardKnowledgeV2Command,
                CreateCardKnowledgeV2UseCase,
            )

            mutation = await CreateCardKnowledgeV2UseCase().execute(
                CreateCardKnowledgeV2Command(command.board_id, command.data),
                actor=actor,
                uow=uow,
            )
            return CreateCardInBoardResult(None, knowledge_mutation=mutation)

        service = uow.services.cards
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

    async def preflight(
        self,
        command: GetBoardColumnsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> Any:
        """Authorize a columns read without hydrating the legacy board graph.

        The paginated columns transport performs this lightweight check before
        opening its data-statement budget.  The legacy ``execute`` path remains
        unchanged and continues to return the fully hydrated board object.
        """

        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise EntityNotFoundError("board", command.board_id)
        return board

    async def execute(
        self,
        command: GetBoardColumnsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetBoardColumnsResult:
        board = await _load_readable_board(uow, command.board_id, actor)
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        return GetBoardColumnsResult(board)


# --- archive / restore tree -------------------------------------------------


_ARCHIVE_ENTITY_TYPES = {"ideation", "refinement", "spec"}


def _validate_archive_entity_type(entity_type: str) -> None:
    if entity_type not in _ARCHIVE_ENTITY_TYPES:
        raise ValueError(
            f"Invalid entity_type: {entity_type}. Must be ideation, refinement, or spec."
        )


async def _archive_board_write_allowed(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
) -> bool:
    """Authorize the board before resolving the globally addressed tree root.

    MCP authentication already proves the agent's board grant and carries that
    board in ``ActorContext``.  REST users must own the board or hold an
    editor/admin share; viewer shares remain read-only.
    """

    return (
        await load_accessible_board(
            uow,
            board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        is not None
    )


async def _resolve_archive_root(
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    entity_type: str,
    entity_id: str,
) -> Any | None:
    if entity_type == "ideation":
        root = await uow.services.ideations.get_ideation(entity_id)
    elif entity_type == "refinement":
        root = await uow.services.refinements.get_refinement(entity_id)
    else:
        root = await uow.services.specs.get_spec(entity_id)
    if root is None or getattr(root, "board_id", None) != board_id:
        return None
    return root


async def _preflight_archive_root(
    uow: PulseUnitOfWork,
    command: Any,
    actor: ActorContext,
) -> None:
    _validate_archive_entity_type(command.entity_type)
    if not await _archive_board_write_allowed(uow, command.board_id, actor):
        raise EntityNotFoundError(command.entity_type, command.entity_id)
    if (
        await _resolve_archive_root(
            uow,
            board_id=command.board_id,
            entity_type=command.entity_type,
            entity_id=command.entity_id,
        )
        is None
    ):
        raise EntityNotFoundError(command.entity_type, command.entity_id)


async def _record_tree_activity(
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    actor: ActorContext,
    action: str,
    entity_type: str,
    entity_id: str,
    counts: dict[str, int],
) -> None:
    await uow.services.boards._log_activity(
        board_id=board_id,
        card_id=None,
        action=action,
        actor_type="agent" if actor.source == "mcp" else "user",
        actor_id=actor.actor_id,
        actor_name=actor.actor_name or actor.actor_id,
        details={
            "entity_type": entity_type,
            "entity_id": entity_id,
            "counts": counts,
        },
    )


class ArchiveTreeCommand:
    __slots__ = ("board_id", "entity_type", "entity_id", "record_activity")

    def __init__(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
        *,
        record_activity: bool = False,
    ) -> None:
        self.board_id = board_id
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.record_activity = record_activity


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
        self, command: ArchiveTreeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ArchiveTreeResult:
        await _preflight_archive_root(uow, command, actor)
        counts = await uow.services.archives.archive_tree(
            command.entity_type, command.entity_id
        )
        if command.record_activity:
            await _record_tree_activity(
                uow,
                board_id=command.board_id,
                actor=actor,
                action="tree_archived",
                entity_type=command.entity_type,
                entity_id=command.entity_id,
                counts=counts,
            )
        await commit(uow)
        return ArchiveTreeResult(counts)


class RestoreTreeCommand:
    __slots__ = ("board_id", "entity_type", "entity_id", "record_activity")

    def __init__(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
        *,
        record_activity: bool = False,
    ) -> None:
        self.board_id = board_id
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.record_activity = record_activity


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
        self, command: RestoreTreeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> RestoreTreeResult:
        await _preflight_archive_root(uow, command, actor)
        counts = await uow.services.archives.restore_tree(
            command.entity_type, command.entity_id
        )
        if command.record_activity:
            await _record_tree_activity(
                uow,
                board_id=command.board_id,
                actor=actor,
                action="tree_restored",
                entity_type=command.entity_type,
                entity_id=command.entity_id,
                counts=counts,
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
        self, command: ShareBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ShareBoardResult:
        await _require_readable_board(uow, command.board_id, actor)
        share = await uow.services.shares.share_board(
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
        self,
        command: ListBoardSharesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListBoardSharesResult:
        service = uow.services.shares
        perm = await service.get_user_permission(
            command.board_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not perm:
            raise EntityNotFoundError("board", command.board_id)
        return ListBoardSharesResult(await service.list_shares(command.board_id))


class UpdateBoardShareCommand:
    __slots__ = ("board_id", "share_id", "data")

    def __init__(self, board_id: str, share_id: str, data: Any) -> None:
        self.board_id = board_id
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
        self,
        command: UpdateBoardShareCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> UpdateBoardShareResult:
        await _require_readable_board(uow, command.board_id, actor)
        await _require_share_on_board(uow, command.board_id, command.share_id)
        share = await uow.services.shares.update_share(
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
    __slots__ = ("board_id", "share_id")

    def __init__(self, board_id: str, share_id: str) -> None:
        self.board_id = board_id
        self.share_id = share_id


class RevokeBoardShareResult:
    __slots__ = ()


class RevokeBoardShareUseCase:
    """Revoke a share — owner/admin can revoke, the shared user can leave (write).
    Delegates to ``ShareService.revoke_share`` (the authorization stays there). A
    ``False`` return becomes ``PermissionDeniedError`` carrying the legacy 403
    detail; commits after the revoke."""

    async def execute(
        self,
        command: RevokeBoardShareCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> RevokeBoardShareResult:
        await _require_readable_board(uow, command.board_id, actor)
        await _require_share_on_board(uow, command.board_id, command.share_id)
        revoked = await uow.services.shares.revoke_share(
            command.share_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
        )
        if not revoked:
            raise PermissionDeniedError("Not authorized to revoke this share")
        await commit(uow)
        return RevokeBoardShareResult()
