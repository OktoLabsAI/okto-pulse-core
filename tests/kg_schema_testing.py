"""Test-owned access to the Community local graph runtime."""

from contextlib import contextmanager
from typing import Any

from okto_pulse.community.adapters.graph_ddl import (
    COMMON_NODE_ATTRIBUTES as _COMMON_NODE_ATTRS,
    build_multi_rel_ddl as _build_multi_rel_ddl,
    build_node_ddl as _build_node_ddl,
    build_rel_ddl as _build_rel_ddl,
)
from okto_pulse.community.adapters.kg_runtime import (
    BoardConnection,
    BoardGraphHandle,
    CAPI_SHARED_LIB_MISSING_MARKER,
    CORRUPT_DB_ERROR_MARKERS,
    GRAPH_DB_FILENAME,
    _is_ladybug_corruption_error,
    _open_kuzu_db,
    apply_ladybug_lifecycle_step,
    board_kuzu_path,
    bootstrap_board_graph,
    close_all_connections,
    close_board_db_cache,
    ensure_board_graph_bootstrapped,
    load_vector_extension,
    migrate_schema_for_board,
    open_board_connection,
    open_board_connection_raw,
    purge_board_graph_storage,
    reset_bootstrap_cache_for_tests,
)
from okto_pulse.community.adapters.kuzu_graph_transaction import _materialize
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
    "BoardConnection",
    "BoardGraphHandle",
    "CAPI_SHARED_LIB_MISSING_MARKER",
    "CORRUPT_DB_ERROR_MARKERS",
    "EDGE_LAYERS",
    "EDGE_METADATA_COLUMNS",
    "GRAPH_DB_FILENAME",
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
    "_is_ladybug_corruption_error",
    "_open_kuzu_db",
    "apply_ladybug_lifecycle_step",
    "board_kuzu_path",
    "bootstrap_board_graph",
    "close_all_connections",
    "close_board_db_cache",
    "ensure_board_graph_bootstrapped",
    "load_vector_extension",
    "migrate_schema_for_board",
    "open_board_connection",
    "open_board_connection_raw",
    "open_materialized_board_connection",
    "purge_board_graph_storage",
    "relationship_endpoint_pairs",
    "reset_bootstrap_cache_for_tests",
    "resolve_relationship_endpoint_pair",
    "stable_rel_type_entries",
    "vector_index_name",
]


class _MaterializedGraphConnection:
    """Expose the Core graph result contract over a native test connection."""

    def __init__(self, native_connection: Any) -> None:
        self._native_connection = native_connection

    def execute(self, statement: str, params: dict[str, Any] | None = None):
        native_result = (
            self._native_connection.execute(statement, params)
            if params
            else self._native_connection.execute(statement)
        )
        return _materialize(native_result)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._native_connection, name)


@contextmanager
def open_materialized_board_connection(board_id: str):
    with open_board_connection(board_id) as (database, native_connection):
        yield database, _MaterializedGraphConnection(native_connection)
