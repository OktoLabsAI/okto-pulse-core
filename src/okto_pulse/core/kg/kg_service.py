"""KG Service — shared logic layer consumed by MCP tools and REST endpoints.

Responsibilities:
- ACL enforcement before any query (FR-9): check_board_access(user, board_id)
- Default filters (FR-2): validation_status, min_confidence, max_rows
- Schema version check via BoardMeta node
- Delegates to graph_store (SemanticGraphStore) via the provider registry
- Returns typed dicts; callers (MCP/REST) wrap into Pydantic models

Async methods that call a synchronous graph adapter use ``_run_graph_io`` to
offload the blocking work to a dedicated thread pool, keeping the event loop
responsive under concurrent load.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
    runtime_state,
)

import asyncio
import logging
import threading
import time as _time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import Any

from okto_pulse.core.kg import cypher_templates as tpl
from okto_pulse.core.kg.cursor_codec import decode_cursor
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
from okto_pulse.core.kg.query_contract import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_VALUES,
    RELATED_CONTEXT_DEPTHS,
    RELATED_CONTEXT_DIRECTIONS,
)
from okto_pulse.core.kg.schema_contract import SCHEMA_VERSION

logger = logging.getLogger("okto_pulse.kg.service")

# ---------------------------------------------------------------------------
# Thread pool for offloading synchronous graph adapter IO from the event loop
# ---------------------------------------------------------------------------

_graph_io_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="graph-io")


async def _run_graph_io(func, *args, **kwargs):
    """Run synchronous graph adapter IO in a dedicated thread pool."""
    loop = asyncio.get_running_loop()
    bound = partial(func, *args, **kwargs)
    context = copy_context()
    return await loop.run_in_executor(_graph_io_executor, context.run, bound)


def _as_iso_timestamp(value: Any) -> str | None:
    """Normalize graph TIMESTAMP values at the typed API boundary."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


# ---------------------------------------------------------------------------
# v0.3.0 R2 — hit counter with lazy flush (bounded LRU cache)
# ---------------------------------------------------------------------------

HIT_FLUSH_THRESHOLD = 10
HIT_FLUSH_MAX_AGE_S = 24 * 3600  # 24h in seconds


class _HitCacheRegistry:
    """Shared eviction registry for the three hit-counter dicts.

    Keeps ``_pending_hits``, ``_last_flush``, and ``_hit_locks`` in sync
    under a single OrderedDict-based LRU with configurable max size.
    Uses a threading.Lock — safe because asyncio runs in a single thread.
    """

    def __init__(self, max_size: int = 5000) -> None:
        self._max_size = max_size
        self._order: OrderedDict[tuple[str, str], None] = OrderedDict()
        self._pending_hits: dict[tuple[str, str], int] = {}
        self._last_flush: dict[tuple[str, str], datetime] = {}
        self._hit_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._has_lock: set[tuple[str, str]] = set()
        self._lock = threading.Lock()

    def get_pending(self, key: tuple[str, str]) -> int:
        """Return pending-hit count, creating the entry if absent."""
        with self._lock:
            if key not in self._order:
                self._order[key] = None
                self._pending_hits[key] = 0
            else:
                self._order.move_to_end(key)
            return self._pending_hits[key]

    def set_pending(self, key: tuple[str, str], value: int) -> None:
        """Set pending-hit count; evicts oldest if over capacity."""
        with self._lock:
            if key in self._order:
                self._order.move_to_end(key)
            else:
                self._order[key] = None
            self._pending_hits[key] = value
            self._evict()

    def get_flush(self, key: tuple[str, str]) -> datetime | None:
        """Return last-flush timestamp without touching LRU order."""
        return self._last_flush.get(key)

    def set_flush(self, key: tuple[str, str], value: datetime) -> None:
        """Set last-flush timestamp; evicts oldest if over capacity."""
        with self._lock:
            if key in self._order:
                self._order.move_to_end(key)
            else:
                self._order[key] = None
            self._last_flush[key] = value
            self._evict()

    def get_lock(self, key: tuple[str, str]) -> asyncio.Lock:
        """Return per-node lock, creating it lazily if absent."""
        with self._lock:
            if key not in self._order:
                self._order[key] = None
                self._pending_hits[key] = 0
            else:
                self._order.move_to_end(key)
            if key not in self._has_lock:
                self._hit_locks[key] = asyncio.Lock()
                self._has_lock.add(key)
            return self._hit_locks[key]

    def clear(self) -> None:
        """Clear all caches."""
        with self._lock:
            self._order.clear()
            self._pending_hits.clear()
            self._last_flush.clear()
            self._hit_locks.clear()
            self._has_lock.clear()

    def snapshot(self) -> dict[tuple[str, str], int]:
        """Return a shallow copy of pending hits. For debugging/metrics."""
        return dict(self._pending_hits)

    def _evict(self) -> None:
        """Remove the oldest entry from ALL dicts. Must be called with lock held."""
        while len(self._order) > self._max_size:
            oldest_key, _ = self._order.popitem(last=False)
            self._pending_hits.pop(oldest_key, None)
            self._last_flush.pop(oldest_key, None)
            self._hit_locks.pop(oldest_key, None)
            self._has_lock.discard(oldest_key)


# Module-level registry — single instance shared by all three proxies.
_registry = runtime_state(
    "kg.kg_service.hit_cache_registry",
    lambda: _HitCacheRegistry(max_size=5000),
)


# ---------------------------------------------------------------------------
# Backward-compatible proxy objects
# ---------------------------------------------------------------------------

class _PendingHitsProxy:
    """Proxy for _PENDING_HITS supporting ``d[key]``, ``d[key] += 1``, ``d.get(k)``."""

    def __getitem__(self, key: tuple[str, str]) -> int:
        return _registry.get_pending(key)

    def __setitem__(self, key: tuple[str, str], value: int) -> None:
        _registry.set_pending(key, value)

    def get(self, key: tuple[str, str], default: int = 0) -> int:
        return _registry._pending_hits.get(key, default)


class _LastFlushProxy:
    """Proxy for _LAST_FLUSH supporting ``d[key]``, ``d.get(k)``."""

    def __getitem__(self, key: tuple[str, str]) -> datetime:
        val = _registry.get_flush(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: tuple[str, str], value: datetime) -> None:
        _registry.set_flush(key, value)

    def get(self, key: tuple[str, str], default: Any = None) -> datetime | None:
        return _registry.get_flush(key)


class _HitLocksProxy:
    """Proxy for _HIT_LOCKS supporting ``async with d[key]``."""

    class _LockCtx:
        """Async context manager wrapping an asyncio.Lock."""

        def __init__(self, lock: asyncio.Lock) -> None:
            self._lock = lock

        async def __aenter__(self) -> asyncio.Lock:
            await self._lock.acquire()
            return self._lock

        async def __aexit__(self, *args) -> None:
            self._lock.release()

    def __getitem__(self, key: tuple[str, str]) -> _HitLocksProxy._LockCtx:
        return self._LockCtx(_registry.get_lock(key))


_PENDING_HITS = _PendingHitsProxy()
_LAST_FLUSH = _LastFlushProxy()
_HIT_LOCKS = _HitLocksProxy()


def _reset_hit_state_for_tests() -> None:
    """Clear every bit of module-level hit state. Test-only helper."""
    _registry.clear()


def _hits_snapshot() -> dict[tuple[str, str], int]:
    """Return a shallow copy of the pending cache. For debugging/metrics."""
    return _registry.snapshot()


@dataclass(frozen=True)
class DefaultFilters:
    min_confidence: float = 0.5
    max_rows: int = 100
    # v0.3.0 R3: relevance threshold replaces the legacy
    # validation_status_exclude filter. Default 0.3 is below the neutral
    # 0.5 used on insert so newly created nodes still pass the filter —
    # only nodes whose score has decayed / been penalised below 0.3 get
    # excluded from read-side tooling.
    min_relevance: float = 0.3


@dataclass  # NOT frozen: an Exception must stay mutable (Python sets __traceback__ on
# propagation; a frozen dataclass raises FrozenInstanceError -> 500). See DesignSystemError.
class KGToolError(Exception):
    """Typed error for tier primario tools (FR-8)."""

    code: str  # not_found, permission_denied, invalid_param, graph_error, timeout, schema_drift, empty_result, graph_unavailable
    message: str
    details: dict = field(default_factory=dict)

    def __str__(self):
        return f"KGToolError({self.code}): {self.message}"


GRAPH_LAYER_CHOICES = frozenset(GRAPH_LAYER_VALUES)


def normalize_graph_layer(graph_layer: str | None) -> str:
    """Normalize graph-layer query mode for per-board and global KG reads."""
    value = (graph_layer or GRAPH_LAYER_CANONICAL).strip().lower()
    if value not in GRAPH_LAYER_CHOICES:
        raise KGToolError(
            code="invalid_param",
            message=(
                "graph_layer must be one of "
                "'canonical', 'working', or 'all'"
            ),
            details={"graph_layer": graph_layer},
        )
    return value


# Ranking weights (FR-5): configurable, defaults sum to 1.0.
@dataclass
class RankingWeights:
    semantic: float = 0.5
    graph_centrality: float = 0.2
    recency_decay: float = 0.2
    confidence: float = 0.1


def _get_graph_store():
    """Return the graph_store from the registry."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    store = get_kg_registry().graph_store
    if store is None:
        raise KGToolError(
            code="graph_error",
            message="graph_store not configured in KG registry",
    )
    return store


def _get_cypher_executor():
    """Return the cypher_executor from the registry."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return get_kg_registry().cypher_executor


def _run_async_blocking(coro):
    """Run a coroutine from sync KG helpers without assuming event-loop shape."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: dict[str, Any] = {}

    def _runner() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - re-raised below
            box["error"] = exc

    context = copy_context()
    thread = threading.Thread(target=context.run, args=(_runner,), daemon=True)
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")


def _execute_graph_write_sync(
    board_id: str,
    cypher: str,
    params: dict[str, Any] | None = None,
) -> None:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    async def _run() -> None:
        async with await get_kg_registry().graph_transaction.begin(board_id) as scope:
            scope.execute(cypher, params or {})

    _run_async_blocking(_run())


def _filters(
    min_confidence: float | None = None,
    max_rows: int | None = None,
    min_relevance: float | None = None,
    defaults: DefaultFilters | None = None,
) -> QueryFilters:
    """Build QueryFilters from optional overrides + service defaults."""
    d = defaults or DefaultFilters()
    return QueryFilters(
        min_confidence=min_confidence if min_confidence is not None else d.min_confidence,
        max_rows=max_rows if max_rows is not None else d.max_rows,
        min_relevance=min_relevance if min_relevance is not None else d.min_relevance,
    )


def _flush_to_graph(
    board_id: str, node_type: str, node_id: str, delta: int, now_iso: str,
) -> None:
    """Sync helper: write hit counter delta to graph backend (runs in thread pool)."""
    _execute_graph_write_sync(
        board_id,
        f"MATCH (n:{node_type} {{id: $nid}}) "
        f"SET n.query_hits = COALESCE(n.query_hits, 0) + $delta, "
        f"n.last_queried_at = $ts",
        {"nid": node_id, "delta": delta, "ts": now_iso},
    )


async def _emit_hit_flushed_event(
    board_id: str, node_type: str, node_id: str, delta: int, now_iso: str,
) -> None:
    """Fire-and-forget publisher for KGHitFlushed (spec 28583299, IMPL-B).

    Opens an edition UnitOfWork so the search hot path does not have to carry
    a relational context through internal helpers. Best-effort: any
    failure (no UoW provider in test mode, dispatcher down, etc.) is
    logged and swallowed because the event is operational telemetry — the
    underlying hit flush has already succeeded.
    """
    try:
        from okto_pulse.core.events.types import KGHitFlushed
        from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
    except Exception as exc:  # pragma: no cover — import-time guard
        logger.debug(
            "kg.hit_flushed.import_failed err=%s", exc,
        )
        return

    try:
        factory = resolve_unit_of_work_factory()
    except RuntimeError:
        # No DB initialised (sync test suites that exercise scoring only).
        return

    event = KGHitFlushed(
        board_id=board_id,
        node_type=node_type,
        node_id=node_id,
        hits_delta=int(delta),
        flushed_at=now_iso,
    )
    try:
        realm_scope = factory.resolve_realm_scope()
        async with factory(realm_scope=realm_scope) as uow:
            await uow.services.publish_domain_event(event)
            await uow.commit()
    except Exception as exc:
        logger.warning(
            "kg.hit_flushed.publish_failed board=%s node=%s err=%s",
            board_id, node_id, exc,
            extra={
                "event": "kg.hit_flushed.publish_failed",
                "board_id": board_id,
                "node_id": node_id,
                "error": str(exc),
            },
        )


class KGService:
    """Stateless service layer. Instantiate per-request or share across calls."""

    def __init__(
        self,
        *,
        default_filters: DefaultFilters | None = None,
        ranking_weights: RankingWeights | None = None,
        emit_hit_events: bool = True,
    ):
        self.defaults = default_filters or DefaultFilters()
        self.weights = ranking_weights or RankingWeights()
        self.emit_hit_events = emit_hit_events

    # ------------------------------------------------------------------
    # ACL (FR-9)
    # ------------------------------------------------------------------

    def check_board_access(self, user_boards: list[str], board_id: str) -> None:
        """Raise KGToolError(permission_denied) if user doesn't have access."""
        if board_id not in user_boards:
            raise KGToolError(
                code="permission_denied",
                message=f"No access to board {board_id}",
            )

    # ------------------------------------------------------------------
    # Hit counter (v0.3.0 R2 — FR5/FR9)
    # ------------------------------------------------------------------

    async def increment_hit(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
    ) -> None:
        """Record that ``node_id`` appeared in a query result top-K.

        Lazy-flushes the counter to graph backend when the pending count reaches
        ``HIT_FLUSH_THRESHOLD`` (10) or when the last flush was more than
        24h ago. R3 wires this into the hybrid_search top-K; R2 exposes
        it as a public method that tests can exercise directly.

        Thread-safety: a per-node ``asyncio.Lock`` serialises increments
        against the same node without blocking increments on other nodes.
        A crash between increments and the next flush loses at most
        ``HIT_FLUSH_THRESHOLD`` hits per node (documented trade-off — BR3).
        """
        key = (board_id, node_id)
        async with _HIT_LOCKS[key]:
            _PENDING_HITS[key] += 1
            count = _PENDING_HITS[key]
            last_flush = _LAST_FLUSH.get(key)
            age_s = (datetime.now(timezone.utc) - last_flush).total_seconds() if last_flush else None

            should_flush = count >= HIT_FLUSH_THRESHOLD or (
                age_s is not None and age_s >= HIT_FLUSH_MAX_AGE_S
            )
            if should_flush:
                await self._flush_hits(board_id, node_type, node_id)

    async def _flush_hits(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
    ) -> None:
        """Write the pending hit counter to graph backend. Caller holds the lock."""
        key = (board_id, node_id)
        delta = _PENDING_HITS.get(key, 0)
        if delta <= 0:
            _LAST_FLUSH[key] = datetime.now(timezone.utc)
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            await _run_graph_io(
                _flush_to_graph, board_id, node_type, node_id, delta, now_iso,
            )
        except Exception as exc:
            logger.error(
                "kg.scoring.hit_flush_failed board=%s node=%s delta=%d err=%s",
                board_id, node_id, delta, exc,
                extra={
                    "event": "kg.scoring.hit_flush_failed",
                    "board_id": board_id,
                    "node_id": node_id,
                    "delta": delta,
                },
            )
            # Reset anyway — BR3 ACKs that hits can be lost on failure
            # rather than risk unbounded cache growth on persistent error.
            _PENDING_HITS[key] = 0
            _LAST_FLUSH[key] = datetime.now(timezone.utc)
            return

        logger.info(
            "kg.scoring.hit_flushed board=%s node=%s delta=%d",
            board_id, node_id, delta,
            extra={
                "event": "kg.scoring.hit_flushed",
                "board_id": board_id,
                "node_id": node_id,
                "delta": delta,
            },
        )
        _PENDING_HITS[key] = 0
        _LAST_FLUSH[key] = datetime.now(timezone.utc)

        if not self.emit_hit_events:
            return

        # spec 28583299 (Ideação #4, BR3 + dec_3a6eb8ad): emit KGHitFlushed
        # via fire-and-forget so the recompute handler ranks the refreshed
        # hits without blocking the search hot path. Independent session
        # (B-1.b in KE-B) avoids polluting the search call-chain. Fire-and-
        # forget is safe — flush failure already discards counts (line 383),
        # and the worst case for losing the event is the score lagging by
        # one tick (decay half-life is 30 days, so seconds don't matter).
        try:
            asyncio.create_task(
                _emit_hit_flushed_event(
                    board_id, node_type, node_id, delta, now_iso,
                )
            )
        except RuntimeError:
            # No running loop (sync test contexts) — ignore. The event is
            # operational telemetry, not a correctness invariant.
            pass

    # ------------------------------------------------------------------
    # Schema version (FR-6)
    # ------------------------------------------------------------------

    def get_schema_version(self, board_id: str) -> str | None:
        store = _get_graph_store()
        return store.get_schema_version(board_id)

    def check_schema_version(self, board_id: str) -> None:
        ver = self.get_schema_version(board_id)
        if ver and ver != SCHEMA_VERSION:
            raise KGToolError(
                code="schema_drift",
                message=f"Board schema {ver} != expected {SCHEMA_VERSION}",
                details={"board_version": ver, "expected": SCHEMA_VERSION},
            )

    # ------------------------------------------------------------------
    # Cache-aware query helper
    # ------------------------------------------------------------------

    def _cached_call(
        self,
        tool_name: str,
        board_id: str,
        cache_params: dict[str, Any],
        fn,
        *,
        use_cache: bool = True,
    ):
        """Execute fn() with optional read-through cache and metrics."""
        from okto_pulse.core.kg.cache import emit_tool_metrics
        from okto_pulse.core.kg.graph_availability import open_or_classify
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        cache = get_kg_registry().require_cache_backend()
        t0 = _time.monotonic()

        if use_cache and tool_name:
            hit, cached = cache.get(tool_name, board_id, cache_params)
            if hit:
                dur = (_time.monotonic() - t0) * 1000
                emit_tool_metrics(
                    tool_name=tool_name, board_id=board_id,
                    cache_hit=True, duration_ms=dur,
                    result_count=len(cached) if isinstance(cached, list) else 1,
                )
                return cached

        try:
            # open_or_classify turns a fail-closed graph-open failure into a
            # typed KGToolError(code="graph_unavailable"); any other failure
            # propagates unchanged and is flattened to graph_error below (FR6).
            result = open_or_classify(fn, board_id=board_id)
        except Exception as exc:
            dur = (_time.monotonic() - t0) * 1000
            is_unavailable = (
                isinstance(exc, KGToolError) and exc.code == "graph_unavailable"
            )
            error_code = "graph_unavailable" if is_unavailable else "graph_error"
            if tool_name:
                emit_tool_metrics(
                    tool_name=tool_name, board_id=board_id,
                    cache_hit=False, duration_ms=dur,
                    result_count=0, error_code=error_code,
                )
            if is_unavailable:
                raise
            raise KGToolError(
                code="graph_error",
                message=f"Query failed: {exc}",
            ) from exc

        if use_cache and tool_name:
            cache.put(tool_name, board_id, cache_params, result)

        dur = (_time.monotonic() - t0) * 1000
        if tool_name:
            emit_tool_metrics(
                tool_name=tool_name, board_id=board_id,
                cache_hit=False, duration_ms=dur,
                result_count=len(result) if isinstance(result, list) else 1,
            )
        return result

    # ------------------------------------------------------------------
    # 0a. get_node_detail (visualization — any node type)
    # ------------------------------------------------------------------

    def get_node_detail(self, board_id: str, node_id: str) -> dict | None:
        """Fetch one node by id across any node type in the per-board graph.

        Tries each NODE_TYPES table in turn (graph backend has no polymorphic MATCH).
        Returns the first hit with the shape expected by the KGNode frontend
        type; `None` when the id isn't present in any table.
        """
        from okto_pulse.core.kg.schema_contract import NODE_TYPES

        logger.debug("[KG] KGService.get_node_detail board_id=%s node_id=%s", board_id, node_id)
        cypher_executor = _get_cypher_executor()
        for ntype in NODE_TYPES:
            cypher = (
                f"MATCH (n:{ntype} {{id: $nid}}) "
                f"RETURN n.id, n.title, n.content, n.justification, "
                f"n.source_artifact_ref, n.source_confidence, "
                f"n.relevance_score, n.query_hits, n.last_queried_at, "
                f"n.created_at, n.superseded_by"
            )
            try:
                result = cypher_executor.execute_read_only(
                    board_id, cypher, {"nid": node_id}, max_rows=1
                )
                rows = result.get("rows", [])
                if rows:
                    r = rows[0]
                    return {
                        "id": r[0],
                        "title": r[1] or "",
                        "content": r[2] or "",
                        "justification": r[3] or "",
                        "source_artifact_ref": r[4],
                        "source_confidence": r[5] if r[5] is not None else 0.0,
                        "relevance_score": r[6] if r[6] is not None else 0.5,
                        "query_hits": r[7] if r[7] is not None else 0,
                        "last_queried_at": r[8],
                        "created_at": r[9].isoformat() if r[9] else None,
                        "superseded_by": r[10],
                        "node_type": ntype,
                    }
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------
    # 0. get_all_nodes (visualization — all types)
    # ------------------------------------------------------------------

    def get_all_nodes(
        self,
        board_id: str,
        *,
        min_confidence: float = 0.0,
        max_rows: int | None = None,
        cursor: str | None = None,
        min_relevance: float | None = None,
        node_type: str | None = None,
        graph_layer: str = GRAPH_LAYER_CANONICAL,
    ) -> list[dict]:
        """Return nodes ordered ``(created_at DESC, id DESC)`` — Spec 8 / S1.3.

        When ``cursor`` is provided it must be a string produced by
        :func:`okto_pulse.core.kg.cursor_codec.encode_cursor`; the query then
        returns rows strictly "after" that cursor in the stable order.
        """
        layer = normalize_graph_layer(graph_layer)
        f = _filters(min_confidence, max_rows, min_relevance, self.defaults)
        params: dict = {
            "min_confidence": f.min_confidence,
            "max_rows": f.max_rows,
            "min_relevance": f.min_relevance,
            "graph_layer": layer,
        }
        if node_type:
            params["node_type"] = node_type
        if cursor:
            cursor_ts, cursor_id = decode_cursor(cursor)
            params["cursor_ts"] = cursor_ts
            params["cursor_id"] = cursor_id
            template = (
                tpl.GET_ALL_NODES_BY_TYPE_AFTER_CURSOR
                if node_type else tpl.GET_ALL_NODES_AFTER_CURSOR
            )
        else:
            template = tpl.GET_ALL_NODES_BY_TYPE if node_type else tpl.GET_ALL_NODES

        def _query():
            result = _get_cypher_executor().execute_read_only(
                board_id, template, params, max_rows=f.max_rows
            )
            return result.get("rows", [])

        rows = self._cached_call("get_all_nodes", board_id, params, _query)
        return [
            {
                "id": r[0], "node_type": r[1], "title": r[2], "content": r[3],
                "created_at": _as_iso_timestamp(r[4]), "source_confidence": r[5],
                "relevance_score": r[6] if r[6] is not None else 0.5,
                "source_artifact_ref": r[7],
                "graph_layer": r[8] if len(r) > 8 and r[8] else "legacy_unknown",
                "maturity_status": r[9] if len(r) > 9 else None,
            }
            for r in rows
        ]

    def count_all_nodes(
        self,
        board_id: str,
        *,
        min_confidence: float = 0.0,
        min_relevance: float | None = None,
        node_type: str | None = None,
        graph_layer: str = GRAPH_LAYER_CANONICAL,
    ) -> int:
        """Count nodes matching the same filters as ``get_all_nodes``.

        The REST ``/nodes`` endpoint exposes this as ``total_hint`` so callers
        can distinguish page size from the total filtered result size.
        """
        layer = normalize_graph_layer(graph_layer)
        f = _filters(min_confidence, None, min_relevance, self.defaults)
        params: dict = {
            "min_confidence": f.min_confidence,
            "min_relevance": f.min_relevance,
            "graph_layer": layer,
        }
        if node_type:
            params["node_type"] = node_type
            template = tpl.COUNT_ALL_NODES_BY_TYPE
        else:
            template = tpl.COUNT_ALL_NODES

        def _query():
            result = _get_cypher_executor().execute_read_only(
                board_id, template, params, max_rows=1
            )
            rows = result.get("rows", [])
            if rows:
                row = rows[0]
                return int(row[0] if isinstance(row, (list, tuple)) else row)
            return 0

        return int(self._cached_call("count_all_nodes", board_id, params, _query))

    # ------------------------------------------------------------------
    # 1. get_decision_history (FR-11)
    # ------------------------------------------------------------------

    def get_decision_history(
        self,
        board_id: str,
        topic: str,
        *,
        min_confidence: float | None = None,
        max_rows: int | None = None,
        use_semantic: bool = True,
        min_similarity: float = 0.3,
    ) -> list[dict]:
        """Trace decisions about a topic.

        When ``use_semantic=True`` (default) the topic is embedded and the
        Decision HNSW index is queried — so paraphrases like "cache strategy"
        vs "caching approach" surface relevant matches. Results missing from
        the vector index (empty content, corrupted embedding) fall back to the
        legacy title-CONTAINS match so no decision becomes invisible.

        When ``use_semantic=False`` only title-CONTAINS is used (preserved for
        callers that want deterministic string matching).
        """
        logger.debug("[KG] KGService.get_decision_history board_id=%s topic=%r use_semantic=%s",
                     board_id, topic, use_semantic)
        store = _get_graph_store()
        f = _filters(min_confidence, max_rows, defaults=self.defaults)

        # Text match first — deterministic, always-available, low cost. Semantic
        # enrichment runs only if we still have budget (fewer hits than max_rows)
        # so the happy-path performance and test ergonomics match the legacy
        # behavior when use_semantic is effectively a no-op.
        text_rows = self._cached_call(
            "get_decision_history", board_id, {"topic": topic},
            lambda: store.find_by_topic(board_id, "Decision", topic, f),
        )

        semantic_rows: list[list] = []
        needs_semantic = (
            use_semantic
            and bool(topic.strip())
            and len(text_rows) < f.max_rows
            and hasattr(store, "find_by_topic_semantic")
        )
        if needs_semantic:
            try:
                from okto_pulse.core.kg.embedding import get_embedding_provider

                query_vec = get_embedding_provider().encode(topic)
                semantic_rows = self._cached_call(
                    "get_decision_history.semantic", board_id,
                    {"topic": topic, "top_k": f.max_rows},
                    lambda: store.find_by_topic_semantic(
                        board_id, "Decision", query_vec, f, min_similarity,
                    ),
                )
            except Exception as exc:
                logger.debug(
                    "kg.decision_history.semantic_fallback board=%s err=%s",
                    board_id, exc,
                )
                semantic_rows = []

        # Merge: text hits first (stable ordering), semantic backfills decisions
        # the title-CONTAINS missed. Dedup by id.
        seen: set[str] = set()
        merged: list[list] = []
        for r in text_rows + semantic_rows:
            if r[0] in seen:
                continue
            seen.add(r[0])
            merged.append(r)
            if len(merged) >= f.max_rows:
                break

        return [
            {
                "id": r[0], "title": r[1], "content": r[2],
                "created_at": _as_iso_timestamp(r[3]), "source_confidence": r[4],
                "relevance_score": r[5] if r[5] is not None else 0.5,
                "superseded_by": r[6],
            }
            for r in merged
        ]

    # ------------------------------------------------------------------
    # 2. get_related_context (FR-12)
    # ------------------------------------------------------------------

    def get_related_context(
        self,
        board_id: str,
        artifact_id: str,
        *,
        min_confidence: float | None = None,
        max_rows: int | None = None,
        rel_types: list[str] | None = None,
        direction: str = "both",
        max_depth: int = 2,
        graph_layer: str = GRAPH_LAYER_CANONICAL,
    ) -> list[dict]:
        """2-hop (or 1-hop) neighborhood around an artifact with optional
        relationship-type + direction filters for impact analysis.

        ``rel_types`` — restrict hop1 edges to a subset (e.g.
        ``["supersedes", "contradicts"]``). ``None`` = any edge type.
        ``direction`` — ``"outgoing"``, ``"incoming"``, or ``"both"`` (default).
        Applied to hop1 only; hop2 always undirected to surface the whole
        neighborhood.
        ``max_depth`` — ``1`` returns center+hop1 only (hop2 fields null);
        ``2`` (default) returns up to 2 hops.
        ``graph_layer`` — ``canonical`` (default) | ``working`` | ``all`` (spec
        849d6292, FR6/TR4). The default scopes the neighborhood to canonical
        nodes so a centered subgraph NEVER leaks ``working`` nodes; the value is
        propagated into the store Cypher (center+hop1+hop2), not filtered
        post-hoc, so the non-leakage guarantee holds at the data layer.
        """
        if direction not in RELATED_CONTEXT_DIRECTIONS:
            raise ValueError(
                f"invalid direction {direction!r}: expected 'both', 'incoming', 'outgoing'"
            )
        if max_depth not in RELATED_CONTEXT_DEPTHS:
            raise ValueError(f"invalid max_depth {max_depth!r}: expected 1 or 2")

        layer = normalize_graph_layer(graph_layer)
        store = _get_graph_store()
        f = _filters(min_confidence, max_rows, defaults=self.defaults)

        # Prefer the filtered method when the store implements it; otherwise
        # fall back to the legacy 2-hop undirected query (caller gets a hint
        # in the cache key so caches don't collide between shapes / layers).
        if (
            hasattr(store, "find_by_artifact_filtered")
            and (rel_types is not None or direction != "both" or max_depth != 2)
        ):
            cache_params = {
                "artifact_id": artifact_id,
                "rel_types": sorted(rel_types) if rel_types else None,
                "direction": direction,
                "max_depth": max_depth,
                "graph_layer": layer,
            }
            rows = self._cached_call(
                "get_related_context.filtered", board_id, cache_params,
                lambda: store.find_by_artifact_filtered(
                    board_id, artifact_id, f,
                    rel_types=rel_types, direction=direction, max_depth=max_depth,
                    graph_layer=layer,
                ),
            )
        else:
            rows = self._cached_call(
                "get_related_context", board_id,
                {"artifact_id": artifact_id, "graph_layer": layer},
                lambda: store.find_by_artifact(
                    board_id, artifact_id, f, graph_layer=layer,
                ),
            )
        shaped = [
            {
                "center_id": r[0], "center_title": r[1],
                "hop1_id": r[2], "hop1_title": r[3],
                "hop2_id": r[4], "hop2_title": r[5],
                "rel1_type": r[6], "rel2_type": r[7],
            }
            for r in rows
        ]
        # Spec MKG-C-S1 (FR6/BR4): fold members of ACTIVE equivalences into
        # their survivor post-fetch (composes with the in-Cypher graph_layer
        # scoping above — equivalences live OFF-graph, the store cannot
        # know them). Rows that collapse onto the same hop tuple dedupe.
        from okto_pulse.core.kg.equivalence_fold import (
            fold_rows,
            load_equivalence_mapping,
        )

        mapping = load_equivalence_mapping(board_id)
        if mapping:
            shaped = fold_rows(
                shaped, mapping,
                id_keys=("center_id", "hop1_id", "hop2_id"),
            )
            deduped: list[dict] = []
            seen: set[tuple] = set()
            for row in shaped:
                key = (
                    row["center_id"], row["hop1_id"], row["hop2_id"],
                    row["rel1_type"], row["rel2_type"],
                )
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(row)
            shaped = deduped
        return shaped

    # ------------------------------------------------------------------
    # 3. get_supersedence_chain (FR-15)
    # ------------------------------------------------------------------

    def get_supersedence_chain(
        self,
        board_id: str,
        decision_id: str,
        node_type: str = "Decision",
    ) -> dict:
        # Spec MKG-D-S1 (FR6): generic per-type chain with a NODE_TYPES
        # allowlist (fail-closed) — Decision default keeps the legacy
        # behaviour byte-identical.
        from okto_pulse.core.kg.schema_contract import NODE_TYPES

        if node_type not in NODE_TYPES:
            raise KGToolError(
                code="invalid_node_type",
                message=(
                    f"node_type {node_type!r} is not a KG node type; "
                    f"allowed: {', '.join(NODE_TYPES)}"
                ),
            )
        store = _get_graph_store()
        chain: list[dict] = []
        current_id = decision_id
        visited: set[str] = set()

        def _str(ts):
            """graph backend returns TIMESTAMP as datetime; SupersedenceEntry (pydantic)
            expects ISO strings. Normalise here rather than leaking the raw
            datetime through the API boundary."""
            if ts is None:
                return None
            if hasattr(ts, "isoformat"):
                return ts.isoformat()
            return str(ts)

        for _ in range(10):  # max depth safety
            rows = store.traverse_supersedence(
                board_id, current_id, node_type=node_type
            )
            if not rows:
                break
            next_node = {
                "id": rows[0][0], "title": rows[0][1],
                "created_at": _str(rows[0][2]),
                "superseded_by": rows[0][3],
                "superseded_at": _str(rows[0][4]),
            }
            if next_node["id"] in visited:
                break  # cycle guard
            visited.add(next_node["id"])
            chain.append(next_node)
            current_id = next_node["id"]
        return {
            "chain": chain,
            "depth": len(chain),
            "current_active": decision_id,
        }

    # ------------------------------------------------------------------
    # 4. find_contradictions (FR-14)
    # ------------------------------------------------------------------

    def find_contradictions(
        self,
        board_id: str,
        node_id: str | None = None,
        *,
        max_rows: int | None = None,
    ) -> list[dict]:
        logger.debug("[KG] KGService.find_contradictions board_id=%s node_id=%s", board_id, node_id)
        store = _get_graph_store()
        limit = max_rows or min(50, self.defaults.max_rows)

        rows = self._cached_call(
            "find_contradictions", board_id, {"node_id": node_id},
            lambda: store.find_contradictions(board_id, node_id, limit),
        )
        pairs = [
            {
                "id_a": r[0], "title_a": r[1],
                "id_b": r[2], "title_b": r[3],
                "confidence": r[4],
            }
            for r in rows
        ]
        # Spec MKG-C-S1 (FR6): fold equivalence members; a pair that
        # collapses onto itself (member vs its survivor) is dropped.
        from okto_pulse.core.kg.equivalence_fold import (
            fold_pair_rows,
            load_equivalence_mapping,
        )

        mapping = load_equivalence_mapping(board_id)
        if mapping:
            pairs = fold_pair_rows(pairs, mapping, key_a="id_a", key_b="id_b")
        return pairs

    # ------------------------------------------------------------------
    # 5. find_similar_decisions (FR-13) — HNSW + ranking
    # ------------------------------------------------------------------

    def find_similar_decisions(
        self,
        board_id: str,
        topic: str,
        *,
        top_k: int = 10,
        min_similarity: float = 0.3,
        weights: RankingWeights | None = None,
    ) -> list[dict]:
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        logger.debug("[KG] KGService.find_similar_decisions board_id=%s topic=%r top_k=%d",
                     board_id, topic, top_k)
        w = weights or self.weights
        store = _get_graph_store()
        embedder = get_kg_registry().require_embedding_provider()
        query_vec = embedder.encode(topic)

        # The vector path bypasses _cached_call. search.find_similar_nodes_by_type
        # now re-raises a fail-closed graph-open failure as a typed
        # KGToolError(code="graph_unavailable") instead of swallowing it into a
        # silent [] (FR4/FR5); emit the open-failure metric here (OR or_0a8b78be)
        # before that typed error propagates to the MCP handler.
        _t0 = _time.monotonic()
        try:
            raw = store.vector_search(
                board_id=board_id,
                node_type="Decision",
                query_vec=query_vec,
                top_k=top_k * 2,  # fetch extra for re-ranking
                min_similarity=min_similarity,
                # Find Similar is a canonical knowledge surface.  Working
                # nodes remain available through the separately governed
                # diagnostic graph-layer paths, but must never leak through
                # this default decision-reuse query.
                graph_layer="canonical",
            )
        except KGToolError as exc:
            if exc.code == "graph_unavailable":
                from okto_pulse.core.kg.cache import emit_tool_metrics

                emit_tool_metrics(
                    tool_name="find_similar_decisions", board_id=board_id,
                    cache_hit=False, duration_ms=(_time.monotonic() - _t0) * 1000,
                    result_count=0, error_code="graph_unavailable",
                )
            raise

        results = []
        for r in raw:
            semantic = r["similarity"]
            recency = 0.5  # default when we can't compute age
            confidence = 0.5  # placeholder until we fetch from node

            combined = (
                w.semantic * semantic
                + w.graph_centrality * 0.5  # in-degree placeholder
                + w.recency_decay * recency
                + w.confidence * confidence
            )
            results.append({
                "id": r["node_id"],
                "title": r["title"],
                "source_artifact_ref": r.get("source_artifact_ref"),
                "similarity": semantic,
                "combined_score": round(combined, 4),
            })

        # Spec MKG-C-S1 (FR6): fold equivalence members into the survivor
        # BEFORE the final ranking cut, keeping the best-scoring row.
        from okto_pulse.core.kg.equivalence_fold import (
            fold_rows,
            load_equivalence_mapping,
        )

        mapping = load_equivalence_mapping(board_id)
        if mapping:
            results = fold_rows(
                results, mapping,
                id_keys=("id",), dedupe_key="id", score_key="combined_score",
            )
        results.sort(key=lambda x: x["combined_score"], reverse=True)
        return results[:top_k]

    # ------------------------------------------------------------------
    # 6. explain_constraint (FR-16)
    # ------------------------------------------------------------------

    def explain_constraint(
        self,
        board_id: str,
        constraint_id: str,
    ) -> dict:
        store = _get_graph_store()
        main, origin_rows, violation_rows = store.get_constraint_detail(
            board_id, constraint_id
        )
        if not main:
            raise KGToolError(
                code="not_found",
                message=f"Constraint not found: {constraint_id}",
            )
        r = main[0]
        return {
            "id": r[0], "title": r[1], "content": r[2],
            "justification": r[3], "source_artifact_ref": r[4],
            "source_confidence": r[5],
            "origins": [{"id": o[0], "title": o[1]} for o in origin_rows],
            "violations": [{"id": v[0], "title": v[1]} for v in violation_rows],
        }

    # ------------------------------------------------------------------
    # 7. list_alternatives (FR-17)
    # ------------------------------------------------------------------

    def list_alternatives(
        self,
        board_id: str,
        decision_id: str,
        *,
        max_rows: int | None = None,
    ) -> list[dict]:
        store = _get_graph_store()
        limit = max_rows or self.defaults.max_rows

        rows = self._cached_call(
            "list_alternatives", board_id, {"decision_id": decision_id},
            lambda: store.get_alternatives(board_id, decision_id, limit),
        )
        return [
            {
                "id": r[0], "title": r[1], "content": r[2],
                "justification": r[3], "source_confidence": r[4],
                "source_artifact_ref": r[5],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 8. get_learning_from_bugs (FR-18)
    # ------------------------------------------------------------------

    def get_learning_from_bugs(
        self,
        board_id: str,
        area: str,
        *,
        min_confidence: float | None = None,
        max_rows: int | None = None,
    ) -> list[dict]:
        store = _get_graph_store()
        f = _filters(min_confidence, max_rows, defaults=self.defaults)

        rows = self._cached_call(
            "get_learning_from_bugs", board_id, {"area": area},
            lambda: store.get_learnings_for_area(board_id, area, f),
        )
        return [
            {
                "learning_id": r[0], "learning_title": r[1],
                "learning_content": r[2], "justification": r[3],
                "source_confidence": r[4],
                "bug_id": r[5], "bug_title": r[6],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 9. query_global — delegates to global discovery layer
    # ------------------------------------------------------------------

    def query_global(
        self,
        nl_query: str,
        *,
        user_boards: list[str] | None = None,
        top_k: int = 10,
        min_similarity: float = 0.3,
        graph_layer: str = GRAPH_LAYER_CANONICAL,
    ) -> list[dict]:
        """Cross-board discovery via the global discovery meta-graph.

        Queries ~/.okto-pulse/global/discovery.graph directly — HNSW over
        DecisionDigest.embedding, scoped to the caller's boards via the
        CONTAINS_DECISION edge. Falls back to manual cosine when the HNSW
        index is empty (same failure mode as per-board search).
        """
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        if not user_boards:
            return []

        layer = normalize_graph_layer(graph_layer)
        registry = get_kg_registry()
        embedder = registry.require_embedding_provider()
        global_runtime = registry.require_global_discovery_runtime()
        query_vec = embedder.encode(nl_query)
        scope = list(user_boards)
        # R6-IMP3 rework: widen the HNSW window for EVERY layer (including `all`).
        # the indexed similarity adapter returns the GLOBAL top-k BEFORE the board/layer filter,
        # so with many same-embedding digests across boards a narrow window can crowd
        # out the current board's rows (e.g. drop a board's `working` digest under
        # `graph_layer=all`). A wider window + the linear fallback below keep the
        # board-scoped result complete.
        search_k = max(top_k, min(top_k * 5, 500))

        try:
            from okto_pulse.core.kg.global_discovery_writer import (
                global_discovery_writer_scope,
            )

            with global_discovery_writer_scope(
                operation="query_global.layer_schema_migrate",
                owner_id="kg-query-global-layer-schema",
            ):
                global_runtime.ensure_layer_schema()
        except Exception as exc:
            logger.debug("kg.query_global.layer_schema_migrate_failed err=%s", exc)

        try:
            from okto_pulse.core.kg.global_discovery_writer import (
                global_discovery_writer_scope,
            )

            with global_discovery_writer_scope(
                operation="query_global.vector_search",
                owner_id="kg-query-global-vector-search",
            ):
                results = global_runtime.search_decision_digests(
                    query_vec,
                    board_ids=tuple(scope),
                    graph_layer=layer,
                    top_k=search_k,
                    min_similarity=min_similarity,
                )
        except Exception as exc:
            logger.debug("kg.query_global.failed err=%s", exc)
            return []

        filtered_hnsw: list[dict] = []
        if results:
            filtered_hnsw = self._filter_global_results_to_existing_nodes(results)
            # R6-IMP3 rework: only trust the HNSW page when it (a) lost NO rows to the
            # existing-node filter AND (b) actually FILLED the requested top_k. If the
            # board-scoped HNSW result underfills top_k, the global top-k may have
            # crowded out board rows (or a layer) — fall through to the complete,
            # board+layer-scoped linear scan instead of returning a short/partial page.
            if len(filtered_hnsw) == len(results) and len(filtered_hnsw) >= top_k:
                return filtered_hnsw[:top_k]

        try:
            from okto_pulse.core.kg.global_discovery_writer import (
                global_discovery_writer_scope,
            )

            with global_discovery_writer_scope(
                operation="query_global.exhaustive_search",
                owner_id="kg-query-global-exhaustive-search",
            ):
                exhaustive = global_runtime.search_decision_digests(
                    query_vec,
                    board_ids=tuple(scope),
                    graph_layer=layer,
                    top_k=max(search_k, 500),
                    min_similarity=min_similarity,
                    exhaustive=True,
                )
            return self._filter_global_results_to_existing_nodes(exhaustive)[:top_k]
        except Exception as exc:
            logger.debug("kg.query_global.fallback_failed err=%s", exc)
            return filtered_hnsw[:top_k]

    @staticmethod
    def _filter_global_results_to_existing_nodes(results: list[dict]) -> list[dict]:
        """Drop DecisionDigest rows whose source node is absent from board graph."""

        if not results:
            return results

        ids_by_board: dict[str, set[str]] = {}
        for row in results:
            board_id = row.get("board_id")
            node_id = row.get("id")
            if board_id and node_id:
                ids_by_board.setdefault(str(board_id), set()).add(str(node_id))

        existing_by_board: dict[str, set[str]] = {}
        cypher_executor = _get_cypher_executor()

        for board_id, node_ids in ids_by_board.items():
            try:
                result = cypher_executor.execute_read_only(
                    board_id,
                    "MATCH (n) WHERE n.id IN $ids RETURN n.id",
                    {"ids": list(node_ids)},
                    max_rows=len(node_ids) or 1,
                )
                existing_by_board[board_id] = {
                    str(row[0])
                    for row in result.get("rows", [])
                    if row and row[0]
                }
            except Exception as exc:
                logger.warning(
                    "kg.query_global.source_validation_failed board=%s err=%s",
                    board_id, exc,
                    extra={
                        "event": "kg.query_global.source_validation_failed",
                        "board_id": board_id,
                    },
                )
                existing_by_board[board_id] = set()

        filtered = [
            row for row in results
            if str(row.get("id")) in existing_by_board.get(
                str(row.get("board_id")), set()
            )
        ]
        dropped = len(results) - len(filtered)
        if dropped:
            logger.warning(
                "kg.query_global.stale_digest_filtered count=%d",
                dropped,
                extra={
                    "event": "kg.query_global.stale_digest_filtered",
                    "count": dropped,
                },
            )
        return filtered


# Module-level default instance.
_RUNTIME_KEY = "kg.service.default"


def get_kg_service() -> KGService:
    service = resolve_runtime_value(_RUNTIME_KEY)
    if service is None:
        service = KGService()
        register_runtime_value(_RUNTIME_KEY, service)
    return service


def reset_kg_service_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)
