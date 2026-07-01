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

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)


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
        self, command: ListAuditCommand, *, actor: ActorContext, uow: Any
    ) -> ListAuditResult:
        from okto_pulse.core.kg.dashboard_readers import list_consolidation_audit

        entries = await list_consolidation_audit(
            session_of(uow), command.board_id, limit=command.limit
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
    """Cross-board global discovery search (read, no commit). Resolves the caller's
    boards via ``dashboard_readers.list_all_board_ids`` (community: all boards) and
    delegates the search to ``kg_service.query_global`` after normalizing the layer
    — exactly as the legacy endpoint did, including the order
    (``normalize_graph_layer`` BEFORE the search). ``KGToolError`` from either call
    propagates for the adapter to map."""

    async def execute(
        self, command: GlobalSearchCommand, *, actor: ActorContext, uow: Any
    ) -> GlobalSearchResult:
        from okto_pulse.core.kg.dashboard_readers import list_all_board_ids
        from okto_pulse.core.kg.kg_service import get_kg_service, normalize_graph_layer

        user_board_ids = await list_all_board_ids(session_of(uow))
        layer = normalize_graph_layer(command.graph_layer)
        results = get_kg_service().query_global(
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
        self, command: StartHistoricalCommand, *, actor: ActorContext, uow: Any
    ) -> StartHistoricalResult:
        from okto_pulse.core.kg.governance import start_historical_consolidation

        payload = await start_historical_consolidation(
            session_of(uow), command.board_id
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
        self, command: CancelHistoricalCommand, *, actor: ActorContext, uow: Any
    ) -> CancelHistoricalResult:
        from okto_pulse.core.kg.governance import cancel_historical

        payload = await cancel_historical(session_of(uow), command.board_id)
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
        self, command: GetHistoricalProgressCommand, *, actor: ActorContext, uow: Any
    ) -> GetHistoricalProgressResult:
        from okto_pulse.core.kg.governance import get_historical_progress

        progress = await get_historical_progress(session_of(uow), command.board_id)
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
        self, command: DeleteBoardKgCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteBoardKgResult:
        from okto_pulse.core.kg.governance import right_to_erasure

        counts = await right_to_erasure(session_of(uow), command.board_id)
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
        self, command: ListPendingCommand, *, actor: ActorContext, uow: Any
    ) -> ListPendingResult:
        from okto_pulse.core.kg.dashboard_readers import list_pending_entries

        entries = await list_pending_entries(session_of(uow), command.board_id)
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
        self, command: ListPendingTreeCommand, *, actor: ActorContext, uow: Any
    ) -> ListPendingTreeResult:
        from okto_pulse.core.kg.dashboard_readers import build_pending_tree

        payload = await build_pending_tree(
            session_of(uow), command.board_id, depth=command.depth
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
        self, command: RetryPendingEntryCommand, *, actor: ActorContext, uow: Any
    ) -> RetryPendingEntryResult:
        from okto_pulse.core.kg.governance import retry_pending_entry

        payload = await retry_pending_entry(
            session_of(uow),
            command.board_id,
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


class BoostNodeUseCase:
    """Boost a KG node's ``relevance_score`` (+0.3, clamp [0, 1.5]) and persist its
    ``ConsolidationAudit`` row (write). Delegates the graph read/SET + audit staging
    to ``governance.boost_node`` (which holds the graph + ORM audit coupling, so this
    use case stays relational-boundary clean) and then commits the staged audit via
    the UnitOfWork.

    On a successful boost the audit row persists (bug 547a2aa8 fix — the legacy row
    omitted the NOT-NULL ``artifact_type``/``started_at`` columns, so its commit raised
    IntegrityError and was silently swallowed). The commit stays best-effort only for
    a genuinely unexpected failure on the already-boosted graph: it rolls back the
    (audit-only) staged row and the boost — already persisted in the graph — still
    succeeds, preserving the legacy 200/404/500 contract. A missing node (governance
    returns ``None``) is ``EntityNotFoundError("node", node_id)`` (→ adapter 404
    problem); ``BoostPersistError`` from a failed SET propagates uncaught for the
    adapter (→ 500 ``kuzu_error``)."""

    async def execute(
        self, command: BoostNodeCommand, *, actor: ActorContext, uow: Any
    ) -> BoostNodeResult:
        from okto_pulse.core.kg.governance import boost_node

        payload = await boost_node(
            session_of(uow), command.board_id, command.node_id
        )
        if payload is None:
            raise EntityNotFoundError("node", command.node_id)
        try:
            await commit(uow)
        except Exception:
            # Audit is best-effort — the boost is already persisted in the graph.
            await uow.rollback()
        return BoostNodeResult(payload)
