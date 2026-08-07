"""KG dashboard "light" REST use cases (SaaS Refactor spec R01A REST-FU5-S2).

Transport-free reimplementations of the seven ``api/kg_routes.py`` endpoints that
still bound a raw request session: the two readers (``list_audit``,
``global_search``) and the five governance delegators (``start_historical``,
``cancel_historical_endpoint``, ``historical_progress_endpoint``,
``delete_board_kg``, ``get_settings`` — the last two share the historical-progress
read). Each use case delegates to the EXISTING relational reader
(``kg.dashboard_readers``) or governance function (``kg.governance``); the inline
``select`` lives in those service modules, never here, so this file stays free of
``select`` / ``AsyncSession`` / ORM imports (the relational ratchet gate over
``application/use_cases`` would fail otherwise).

The governance writers (start / cancel / erasure) commit the request session
internally — exactly as the legacy endpoints did when they passed ``db`` straight
through — so the use cases do NOT issue a second ``commit``. The reads issue no
commit. ``KGToolError`` raised by the KG service (``query_global`` /
``normalize_graph_layer``) propagates uncaught for the adapter to map.
"""

from __future__ import annotations

from contextvars import Context, copy_context

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog


async def _require_board_access(
    services: ApplicationServiceCatalog,
    actor: ActorContext,
    board_id: str,
) -> None:
    """Fail closed unless the actor can see the target board through QueryScope."""
    actor_scope = ActorScope.from_context(actor)
    query_scope = actor_scope.query_scope(target_board_id=board_id)
    board = await services.boards.get_board(
        board_id,
        actor_scope.actor_id,
        query_scope=query_scope,
    )
    if board is None:
        raise PermissionDeniedError("Not authorized to access this board")


async def _visible_board_ids(
    services: ApplicationServiceCatalog,
    actor: ActorContext,
) -> list[str]:
    """Resolve the actor's global KG search scope using the board service."""
    actor_scope = ActorScope.from_context(actor)
    boards, _total = await services.boards.list_boards(
        actor_scope.actor_id,
        offset=0,
        limit=10_000_000,
        realm_id=actor_scope.realm_id,
        view="all",
        query_scope=actor_scope.query_scope(),
    )
    scoped = actor_scope.query_scope(
        allowed_board_ids=[board.id for board in boards],
        require_ownership=False,
    )
    return sorted(scoped.allowed_board_ids or ())


# --- list audit (reader) ----------------------------------------------------


class ListAuditCommand:
    __slots__ = ("board_id", "limit")

    def __init__(self, board_id: str, *, limit: int) -> None:
        self.board_id = board_id
        self.limit = limit


class ListAuditResult:
    __slots__ = ("entries",)

    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self.entries = entries


class ListAuditUseCase:
    """List committed consolidation-audit entries for a board (read, no commit).
    Delegates the ``select`` + projection to ``dashboard_readers`` so the use case
    holds no relational symbol; the adapter wraps the entries in the legacy
    ``{"entries": ..., "next_cursor": None}`` envelope."""

    async def execute(
        self, command: ListAuditCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListAuditResult:

        await _require_board_access(uow.services, actor, command.board_id)
        entries = await uow.services.kg.list_consolidation_audit(command.board_id, limit=command.limit
        )
        return ListAuditResult(entries)


# --- global search (reader + KG service) ------------------------------------


class GlobalSearchCommand:
    __slots__ = ("q", "limit", "min_similarity", "graph_layer")

    def __init__(
        self,
        *,
        q: str,
        limit: int,
        min_similarity: float,
        graph_layer: str,
    ) -> None:
        self.q = q
        self.limit = limit
        self.min_similarity = min_similarity
        self.graph_layer = graph_layer


class GlobalSearchResult:
    __slots__ = ("results", "graph_layer")

    def __init__(self, results: list[Any], graph_layer: str) -> None:
        self.results = results
        self.graph_layer = graph_layer


class GlobalSearchUseCase:
    """Cross-board global discovery search (read, no commit).

    Resolves the caller's board visibility through ``ActorScope``/``QueryScope``
    and ``BoardService`` before delegating to ``kg_service.query_global``. An
    unresolved scope is never treated as "all boards".
    """

    async def execute(
        self, command: GlobalSearchCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GlobalSearchResult:
        from okto_pulse.core.services.application_kg import (
            normalize_graph_layer,
            query_global,
        )

        user_board_ids = await _visible_board_ids(uow.services, actor)
        layer = normalize_graph_layer(command.graph_layer)
        results = query_global(
            command.q,
            user_boards=user_board_ids,
            top_k=command.limit,
            min_similarity=command.min_similarity,
            graph_layer=layer,
        )
        return GlobalSearchResult(results, layer)


# --- historical consolidation: start (write) --------------------------------


class StartHistoricalCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class StartHistoricalResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class StartHistoricalUseCase:
    """Start the historical backfill for a board (write). Delegates to
    ``governance.start_historical_consolidation`` (which commits the session
    internally, exactly as the legacy endpoint relied on) and returns its payload
    verbatim."""

    async def execute(
        self, command: StartHistoricalCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> StartHistoricalResult:

        await _require_board_access(uow.services, actor, command.board_id)
        await require_authorization(
            actor,
            PermissionRequirement("kg.admin.historical_consolidation"),
            uow=uow,
            board_id=command.board_id,
        )
        payload = await uow.services.kg.start_historical_consolidation(command.board_id
        )
        return StartHistoricalResult(payload)


# --- historical consolidation: cancel (write) -------------------------------


class CancelHistoricalCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class CancelHistoricalResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class CancelHistoricalUseCase:
    """Cancel the historical backfill for a board (write). Delegates to
    ``governance.cancel_historical`` (which commits internally) and returns its
    payload verbatim."""

    async def execute(
        self, command: CancelHistoricalCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CancelHistoricalResult:

        await _require_board_access(uow.services, actor, command.board_id)
        await require_authorization(
            actor,
            PermissionRequirement("kg.admin.historical_consolidation"),
            uow=uow,
            board_id=command.board_id,
        )
        payload = await uow.services.kg.cancel_historical(command.board_id)
        return CancelHistoricalResult(payload)


# --- historical consolidation: progress (read) ------------------------------
# Shared by the ``historical_progress_endpoint`` and ``get_settings`` adapters —
# both read the same governance progress dict.


class GetHistoricalProgressCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class GetHistoricalProgressResult:
    __slots__ = ("progress",)

    def __init__(self, progress: dict[str, Any]) -> None:
        self.progress = progress


class GetHistoricalProgressUseCase:
    """Return historical-consolidation progress for a board (read, no commit).
    Delegates to ``governance.get_historical_progress`` verbatim."""

    async def execute(
        self, command: GetHistoricalProgressCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetHistoricalProgressResult:

        await _require_board_access(uow.services, actor, command.board_id)
        progress = await uow.services.kg.get_historical_progress(command.board_id)
        return GetHistoricalProgressResult(progress)


# --- right to erasure (write) -----------------------------------------------


class DeleteBoardKgCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class DeleteBoardKgResult:
    __slots__ = ("counts",)

    def __init__(self, counts: dict[str, Any]) -> None:
        self.counts = counts


class DeleteBoardKgUseCase:
    """Wipe all KG data for a board — right-to-erasure (write). Delegates to
    ``governance.right_to_erasure`` (best-effort cascade + SQLite purge, commits
    internally) and returns its counts; the adapter still answers 204 No Content
    regardless of the counts, exactly as the legacy endpoint did."""

    async def execute(
        self, command: DeleteBoardKgCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteBoardKgResult:

        await _require_board_access(uow.services, actor, command.board_id)
        await require_authorization(
            actor,
            PermissionRequirement("kg.admin.wipe_board"),
            uow=uow,
            board_id=command.board_id,
        )
        counts = await uow.services.kg.right_to_erasure(command.board_id)
        return DeleteBoardKgResult(counts)


# ===========================================================================
# Pending queue (spec R01A REST-FU5-S3 — list_pending / list_pending_tree /
# retry_pending_entry)
# ===========================================================================


# --- list pending (reader) --------------------------------------------------


class ListPendingCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class ListPendingResult:
    __slots__ = ("entries",)

    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self.entries = entries


class ListPendingUseCase:
    """List the board's pending consolidation-queue entries (read, no commit).
    Delegates the ``select`` + projection to ``dashboard_readers`` so the use case
    holds no relational symbol; the legacy endpoint's swallow-all error fallback
    (``{"entries": [], "count": 0}``) is preserved by the adapter, which also
    derives the ``count`` from the returned entries."""

    async def execute(
        self, command: ListPendingCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListPendingResult:

        await _require_board_access(uow.services, actor, command.board_id)
        entries = await uow.services.kg.list_pending_entries(command.board_id)
        return ListPendingResult(entries)


# --- list pending tree (reader) ---------------------------------------------


class ListPendingTreeCommand:
    __slots__ = ("board_id", "depth")

    def __init__(self, board_id: str, *, depth: int = 5) -> None:
        self.board_id = board_id
        self.depth = depth


class ListPendingTreeResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class ListPendingTreeUseCase:
    """Hierarchical pending-queue view (read, no commit). Delegates the full
    queue-state fetch + in-Python hierarchy join to ``dashboard_readers`` so the
    use case holds no relational symbol; returns the stable payload dict verbatim
    (``{board_id, depth, total_pending, levels, tree}``) for the adapter."""

    async def execute(
        self, command: ListPendingTreeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListPendingTreeResult:

        await _require_board_access(uow.services, actor, command.board_id)
        payload = await uow.services.kg.build_pending_tree(command.board_id, depth=command.depth
        )
        return ListPendingTreeResult(payload)


# --- retry pending entry (write) --------------------------------------------


class RetryPendingEntryCommand:
    __slots__ = ("board_id", "queue_entry_id", "recursive")

    def __init__(
        self, board_id: str, queue_entry_id: str, *, recursive: bool = False
    ) -> None:
        self.board_id = board_id
        self.queue_entry_id = queue_entry_id
        self.recursive = recursive


class RetryPendingEntryResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class RetryPendingEntryUseCase:
    """Re-queue a failed/done ConsolidationQueue entry (write). Delegates to
    ``governance.retry_pending_entry`` (which mutates, commits the request session
    internally and signals the worker, exactly as the legacy endpoint relied on)
    and returns its payload verbatim. A missing entry (``None`` from governance)
    is ``EntityNotFoundError("queue_entry")`` (→ adapter maps to 404 "queue entry
    not found"); the use case issues NO second commit."""

    async def execute(
        self, command: RetryPendingEntryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> RetryPendingEntryResult:

        await _require_board_access(uow.services, actor, command.board_id)
        payload = await uow.services.kg.retry_pending_entry(command.board_id,
            command.queue_entry_id,
            recursive=command.recursive,
        )
        if payload is None:
            raise EntityNotFoundError("queue_entry", command.queue_entry_id)
        return RetryPendingEntryResult(payload)


# ===========================================================================
# Node relevance boost (spec R01A REST-FU5-S4 — kg_routes.boost_node)
# ===========================================================================


class BoostNodeCommand:
    __slots__ = ("board_id", "node_id")

    def __init__(self, board_id: str, node_id: str) -> None:
        self.board_id = board_id
        self.node_id = node_id


class BoostNodeResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class _BoostFenceHandle:
    """Keep one synchronous guard context alive across async audit finalization."""

    __slots__ = ("closed", "context", "entered", "lease", "manager")

    def __init__(self) -> None:
        self.context: Context = copy_context()
        self.manager: Any = None
        self.lease: Any = None
        self.entered = False
        self.closed = False


def _mutate_boost_graph_inside_fence(
    handle: _BoostFenceHandle,
    *,
    guard_factory: Any,
    kg_service: Any,
    command: BoostNodeCommand,
    actor: ActorContext,
) -> object:
    """Enter the fence and complete graph IO/lifecycle in one worker context."""

    def _run() -> object:
        handle.manager = guard_factory(
            command.board_id,
            operation="kg.node_boost",
            owner_id=actor.actor_id,
            mutation_ref=f"node:{command.node_id}:boost",
        )
        handle.lease = handle.manager.__enter__()
        handle.entered = True
        try:
            return run_async_blocking(
                kg_service.mutate_boost_node_graph(
                    command.board_id,
                    command.node_id,
                    actor_id=actor.actor_id,
                )
            )
        finally:
            # A Ladybug/Kuzu SET may auto-commit before result
            # materialization raises. Apply the lifecycle on every exit from
            # the graph service in the same off-loop guard context.
            handle.lease.ensure_durable()

    return handle.context.run(_run)


def _close_boost_fence_sync(
    handle: _BoostFenceHandle,
    error: BaseException | None,
) -> None:
    """Close the suspended sync context manager in its original Context."""

    if not handle.entered or handle.closed:
        return

    def _close() -> None:
        try:
            if error is None:
                handle.lease.ensure_owned(
                    failure_phase="after_boost_audit_finalize"
                )
        except BaseException as ownership_error:
            try:
                handle.manager.__exit__(
                    type(ownership_error),
                    ownership_error,
                    ownership_error.__traceback__,
                )
            finally:
                handle.closed = True
            raise
        try:
            handle.manager.__exit__(
                type(error) if error is not None else None,
                error,
                error.__traceback__ if error is not None else None,
            )
        finally:
            handle.closed = True

    handle.context.run(_close)


async def _close_boost_fence(
    handle: _BoostFenceHandle,
    error: BaseException | None,
) -> None:
    await run_blocking_graph_io(
        lambda: _close_boost_fence_sync(handle, error),
        task_name="kg.node_boost.fence_exit",
    )


class BoostNodeUseCase:
    """Boost a KG node's ``relevance_score`` (+0.3, clamp [0, 1.5]) and persist its
    ``ConsolidationAudit`` row (write). The KG service exposes a graph-only mutation
    and a no-IO audit-staging step: native graph work runs in a worker, while staging
    and finalizing the request-owned UnitOfWork remain on the request loop.

    On a successful boost the audit row persists (bug 547a2aa8 fix — the legacy row
    omitted the NOT-NULL ``artifact_type``/``started_at`` columns, so its commit raised
    IntegrityError and was silently swallowed). The graph SET, durability lifecycle
    and relational audit finalization share one board-writer fence. The commit stays
    best-effort only for a genuinely unexpected failure on the already-durable graph:
    it rolls back the (audit-only) staged row and the boost still succeeds, preserving
    the legacy 200/404/500 contract. A missing node (governance returns ``None``) is
    ``EntityNotFoundError("node", node_id)`` (→ adapter 404 problem);
    ``BoostPersistError`` from a failed SET propagates uncaught for the adapter
    (→ 500 ``graph_error``)."""

    async def execute(
        self, command: BoostNodeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> BoostNodeResult:
        from okto_pulse.core.kg.guarded_write import guarded_board_write

        await _require_board_access(uow.services, actor, command.board_id)
        handle = _BoostFenceHandle()
        try:
            mutation = await run_blocking_graph_io(
                lambda: _mutate_boost_graph_inside_fence(
                    handle,
                    guard_factory=guarded_board_write,
                    kg_service=uow.services.kg,
                    command=command,
                    actor=actor,
                ),
                task_name="kg.node_boost.graph",
            )
            if mutation is None:
                raise EntityNotFoundError("node", command.node_id)

            # Staging remains on the request loop: it touches the request-owned
            # UnitOfWork but performs no graph or relational network IO.
            payload = uow.services.kg.stage_boost_node_audit(mutation)
            try:
                await commit(uow)
            except Exception:
                # Historical API contract: the audit is best-effort after a
                # durable graph boost. Keep the fence until rollback completes.
                await uow.rollback()
        except BaseException as exc:
            await _close_boost_fence(handle, exc)
            raise
        else:
            await _close_boost_fence(handle, None)
        return BoostNodeResult(payload)
