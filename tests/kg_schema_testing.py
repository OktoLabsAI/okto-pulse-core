"""Test-owned access to the Community Grafx board graph runtime.

The Core suite used to reach a retired embedded graph runtime module directly.
That runtime is gone: Community boards are Grafx databases resolved through a
persisted backend binding, a route resolver and a pooled database handle.

This module is the single test-owned seam over the live Community adapters:

* ``bootstrap_board_graph`` / ``ensure_board_graph_bootstrapped`` initialize the
  board route and stamp the current Grafx schema, synchronously, so ordinary
  (non-async) test setup keeps working.
* ``open_board_connection`` yields ``(database, connection)`` where the
  connection is one real staged Grafx write transaction.  ``execute`` accepts
  Pulse's logical Cypher (logical relationship names included) and returns a
  result object that supports both the row-cursor shape the Core tests use
  (``has_next`` / ``get_next`` / ``close``) and the Core
  ``GraphStatementResult`` shape (``rows`` / ``columns``).
* ``open_materialized_board_connection`` yields the plain Core
  ``GraphStatementResult`` without the cursor shim.

Schema-contract constants and the DDL builders are re-exported unchanged.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

from okto_pulse.community.adapters.graph_ddl import (
    COMMON_NODE_ATTRIBUTES as _COMMON_NODE_ATTRS,
    build_multi_rel_ddl as _build_multi_rel_ddl,
    build_node_ddl as _build_node_ddl,
    build_rel_ddl as _build_rel_ddl,
)
from okto_pulse.core.kg.interfaces.graph_errors import GraphError
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.schema_contract import (
    EDGE_LAYERS,
    EDGE_METADATA_COLUMNS,
    HUMAN_CURATED_COLUMNS,
    KG_LAYER_COLUMNS,
    LAST_RECOMPUTED_COLUMNS,
    LEGACY_NODE_COLUMNS,
    MULTI_REL_TYPES,
    NODE_TYPES,
    PRIORITY_BOOST_COLUMNS,
    REL_TYPES,
    RELEVANCE_COLUMNS,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
    VECTOR_INDEX_TYPES,
    relationship_endpoint_pairs,
    resolve_relationship_endpoint_pair,
    stable_rel_type_entries,
    vector_index_name,
)

__all__ = [
    "EDGE_LAYERS",
    "EDGE_METADATA_COLUMNS",
    "HUMAN_CURATED_COLUMNS",
    "KG_LAYER_COLUMNS",
    "LAST_RECOMPUTED_COLUMNS",
    "LEGACY_NODE_COLUMNS",
    "MULTI_REL_TYPES",
    "NODE_TYPES",
    "PRIORITY_BOOST_COLUMNS",
    "REL_TYPES",
    "RELEVANCE_COLUMNS",
    "SCHEMA_VERSION",
    "STABLE_NODE_PROPERTIES",
    "VECTOR_INDEX_TYPES",
    "_COMMON_NODE_ATTRS",
    "_build_multi_rel_ddl",
    "_build_node_ddl",
    "_build_rel_ddl",
    "board_graph_columns",
    "board_graph_path",
    "board_graph_tables",
    "bootstrap_board_graph",
    "close_all_connections",
    "close_board_db_cache",
    "ensure_board_graph_bootstrapped",
    "graph_composition",
    "migrate_schema_for_board",
    "open_board_connection",
    "open_materialized_board_connection",
    "physical_relationship_table",
    "purge_board_graph_storage",
    "relationship_endpoint_pairs",
    "reset_bootstrap_cache_for_tests",
    "resolve_relationship_endpoint_pair",
    "stable_rel_type_entries",
    "vector_index_name",
]


# ---------------------------------------------------------------------------
# Shared routed Grafx composition
# ---------------------------------------------------------------------------

_COMPOSITION: Any | None = None
_COMPOSITION_KEY: tuple[str, str] | None = None
_BOOTSTRAPPED: set[str] = set()


def _community_settings() -> Any:
    from okto_pulse.community.config import CommunitySettings
    from okto_pulse.core import get_settings

    snapshot = get_settings()
    if isinstance(snapshot, CommunitySettings):
        return snapshot
    model_dump = getattr(snapshot, "model_dump", None)
    if callable(model_dump):
        return CommunitySettings(**model_dump())
    return CommunitySettings()


def _composition_key(settings: Any) -> tuple[str, str]:
    return (
        str(getattr(settings, "data_dir", "")),
        str(getattr(settings, "kg_base_dir", "")),
    )


def graph_composition() -> Any:
    """Return (and cache) the routed Community Grafx graph composition.

    The cache is keyed on the configured storage roots so a test that
    reconfigures settings transparently gets a composition bound to the new
    location instead of the previous temporary directory.
    """

    global _COMPOSITION, _COMPOSITION_KEY
    settings = _community_settings()
    key = _composition_key(settings)
    if _COMPOSITION is None or _COMPOSITION_KEY != key:
        from okto_pulse.community.adapters.routed_graph_composition import (
            build_community_routed_graph_composition,
        )

        _close_composition()
        _COMPOSITION = build_community_routed_graph_composition(settings=settings)
        _COMPOSITION_KEY = key
        _BOOTSTRAPPED.clear()
    return _COMPOSITION


def _pools() -> tuple[Any, ...]:
    if _COMPOSITION is None:
        return ()
    board = _COMPOSITION.board
    pools: list[Any] = [board.grafx_pool]
    pools.extend(board.grafx_read_pools)
    global_graph = getattr(_COMPOSITION, "global_graph", None)
    for name in ("grafx_pool", "grafx_read_pools"):
        value = getattr(global_graph, name, None)
        if value is None:
            continue
        if isinstance(value, tuple):
            pools.extend(value)
        else:
            pools.append(value)
    return tuple(dict.fromkeys(pools))


def _close_composition() -> None:
    for pool in _pools():
        try:
            pool.close_all()
        except Exception:  # noqa: BLE001 - teardown is best effort
            pass


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def bootstrap_board_graph(board_id: str) -> Any:
    """Create the board's Grafx database with the current schema."""

    from okto_pulse.community.adapters.grafx_board_operational import (
        current_grafx_timestamp,
    )
    from okto_pulse.community.adapters.grafx_schema_bootstrap import (
        ensure_current_grafx_board_schema,
    )

    composition = graph_composition()
    try:
        composition.initialize_board_route(board_id)
    except GraphError:
        # A board whose immutable binding survived a purge is recreated through
        # the rebuild lane, which is the only door allowed to rematerialize it.
        composition.rematerialize_board_route(board_id)
    database = _board_database(board_id)
    result = ensure_current_grafx_board_schema(
        database,
        board_id=board_id,
        bootstrapped_at=current_grafx_timestamp(),
    )
    _BOOTSTRAPPED.add(board_id)
    return result


def ensure_board_graph_bootstrapped(board_id: str) -> Any:
    """Idempotent ``bootstrap_board_graph`` with a per-process memo."""

    graph_composition()
    if board_id in _BOOTSTRAPPED:
        return None
    return bootstrap_board_graph(board_id)


def reset_bootstrap_cache_for_tests() -> None:
    """Forget the per-process bootstrap memo (not the storage itself)."""

    _BOOTSTRAPPED.clear()


def migrate_schema_for_board(board_id: str) -> dict[str, Any]:
    """Run the routed Grafx schema migration for one board, synchronously."""

    import asyncio

    composition = graph_composition()
    return asyncio.run(composition.graph_schema_manager.migrate(board_id))


# ---------------------------------------------------------------------------
# Paths and handles
# ---------------------------------------------------------------------------


def board_graph_path(board_id: str) -> Path:
    """Return the board's active Grafx database path.

    ``board_id`` is a single storage segment.  An empty id, a path separator or
    a traversal component never resolves to a path: it is rejected here exactly
    as the binding store rejects it, so a caller cannot escape the data root.
    """

    from okto_pulse.community.adapters.graph_route_resolver import _INITIAL_GENERATION

    if not isinstance(board_id, str) or not board_id:
        raise ValueError("board_id must be non-empty text")
    if "/" in board_id or "\\" in board_id or board_id in {".", ".."}:
        raise ValueError(f"board_id is not a single storage segment: {board_id!r}")

    composition = graph_composition()
    try:
        return composition.resolver.acquire_board_route(board_id).active_path
    except Exception:  # noqa: BLE001 - an unmaterialized board still has a path
        return composition.binding_store.board_grafx_path(
            board_id,
            _INITIAL_GENERATION,
        )


def board_graph_tables(board_id: str) -> dict[str, str]:
    """Return ``{physical table name: "NODE" | "REL"}`` from the live catalog."""

    from okto_pulse.community.adapters.grafx_schema_manifest import (
        PULSE_GRAFX_SCHEMA_MANIFEST,
    )

    kinds = {
        table.name: table.kind.upper()
        for table in PULSE_GRAFX_SCHEMA_MANIFEST.tables
    }
    database = _board_database(board_id)
    return {
        table.name: kinds.get(table.name, "UNKNOWN")
        for table in database.catalog.catalog.tables()
    }


def board_graph_columns(board_id: str, table_name: str) -> set[str]:
    """Return the live column names of one physical table in a board graph."""

    database = _board_database(board_id)
    catalog = database.catalog.catalog
    for table in catalog.tables():
        if table.name == table_name:
            return {str(column.name) for column in table.columns}
    raise AssertionError(f"table not present in board graph catalog: {table_name}")


def physical_relationship_table(
    logical_type: str,
    from_type: str,
    to_type: str,
) -> str:
    """Resolve the physical Grafx table backing one logical endpoint pair."""

    from okto_pulse.community.adapters.grafx_relationship_layout import (
        resolve_relationship_table,
    )

    return resolve_relationship_table(logical_type, from_type, to_type)


def _board_database(board_id: str) -> Any:
    composition = graph_composition()
    snapshot = composition.resolver.acquire_board_route(board_id)
    return composition.board.grafx_pool.get(
        snapshot.active_path,
        page_size=snapshot.page_size,
    )


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


class _CursorResult:
    """Row-cursor shape over a Core ``GraphStatementResult``."""

    __slots__ = ("_result", "_index")

    def __init__(self, result: GraphStatementResult) -> None:
        self._result = result
        self._index = 0

    @property
    def rows(self) -> tuple:
        return self._result.rows

    @property
    def columns(self) -> tuple:
        return self._result.columns

    def has_next(self) -> bool:
        return self._index < len(self._result.rows)

    def get_next(self) -> list[Any]:
        row = self._result.rows[self._index]
        self._index += 1
        return list(row)

    def get_column_names(self) -> list[str]:
        return list(self._result.columns)

    def close(self) -> None:
        self._index = len(self._result.rows)

    def __iter__(self):
        return iter(list(row) for row in self._result.rows)

    def __len__(self) -> int:
        return len(self._result.rows)


class _GraphTestConnection:
    """Execute Pulse logical Cypher on one staged Grafx write transaction."""

    def __init__(self, scope: Any, *, cursor: bool) -> None:
        self._scope = scope
        self._cursor = cursor

    def execute(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        result = self._scope.execute(statement, params)
        return _CursorResult(result) if self._cursor else result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._scope, name)


@contextmanager
def _open_scope(board_id: str, *, cursor: bool):
    from okto_pulse.community.adapters.grafx_commit_provenance import (
        begin_board_write,
    )
    from okto_pulse.community.adapters.grafx_graph_transaction import (
        _GrafxTransactionScope,
        _default_relationship_pairs,
    )
    from okto_pulse.community.adapters.grafx_relationship_layout import (
        resolve_relationship_table,
    )

    ensure_board_graph_bootstrapped(board_id)
    database = _board_database(board_id)
    transaction = begin_board_write(database, board_id, "test_graph_connection")
    scope = _GrafxTransactionScope(
        board_id,
        database,
        transaction,
        lambda _board_id, _phase: None,
        node_types=tuple(NODE_TYPES),
        relationship_pairs=_default_relationship_pairs(),
        relationship_table_resolver=resolve_relationship_table,
    )
    try:
        yield database, _GraphTestConnection(scope, cursor=cursor)
    except BaseException:
        if transaction.active:
            transaction.rollback()
        raise
    else:
        if transaction.active:
            transaction.commit()


@contextmanager
def open_board_connection(board_id: str):
    """Yield ``(database, connection)`` on one staged Grafx write transaction."""

    with _open_scope(board_id, cursor=True) as opened:
        yield opened


@contextmanager
def open_materialized_board_connection(board_id: str):
    """Same as :func:`open_board_connection`, returning Core result objects."""

    with _open_scope(board_id, cursor=False) as opened:
        yield opened


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def close_board_db_cache(board_id: str | None = None) -> None:
    """Drop pooled Grafx handles for one board (or every board)."""

    if _COMPOSITION is None:
        return
    if board_id is None:
        close_all_connections()
        return
    path = board_graph_path(board_id)
    for pool in _pools():
        try:
            pool.close(path)
        except Exception:  # noqa: BLE001 - teardown is best effort
            pass


def close_all_connections() -> None:
    """Close every pooled Grafx handle held by the test composition."""

    _close_composition()


def purge_board_graph_storage(board_id: str, *, reason: str) -> Any:
    """Purge a board's Grafx storage through the routed graph lifecycle.

    Returns the adapter's ``PurgeReport`` (truthy, with the affected storage
    refs) rather than the retired runtime's list of moved filesystem paths.
    """

    import asyncio

    composition = graph_composition()
    report = asyncio.run(
        composition.board.graph_lifecycle.purge(board_id, reason=reason)
    )
    _BOOTSTRAPPED.discard(board_id)
    return report
