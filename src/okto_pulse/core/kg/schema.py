"""Legacy board graph schema facade.

The concrete Ladybug/Kuzu runtime is edition-owned. Core code should consume the
narrow graph ports; this module exists only for backward-compatible imports and
delegates runtime operations to ``KGProviderRegistry.board_graph_runtime``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    _COMMON_NODE_ATTRS,
    _build_multi_rel_ddl,
    _build_node_ddl,
    _build_rel_ddl,
    relationship_endpoint_pairs,
    resolve_relationship_endpoint_pair,
    stable_rel_type_entries,
    vector_index_name,
)

GRAPH_DB_FILENAME = "graph.lbug"
CORRUPT_DB_ERROR_MARKERS = (
    "checksum verification failed",
    "corrupted wal file",
    "wal file is corrupted",
    "invalid wal record",
    "not a valid lbug database file",
    "wal_record.cpp",
    "unreachable_code",
)
CAPI_SHARED_LIB_MISSING_MARKER = "could not find lbug c api shared library"


@dataclass(frozen=True)
class BoardGraphHandle:
    """Compatibility DTO returned by edition-owned bootstrap implementations."""

    board_id: str
    path: Path
    schema_version: str


class BoardConnection:
    """Import-time-safe proxy for the edition-owned BoardConnection class."""

    def __new__(cls, board_id: str) -> Any:
        return _runtime().open_board_connection(board_id)


def _runtime() -> Any:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    runtime = getattr(get_kg_registry(), "board_graph_runtime", None)
    if runtime is None:
        raise RuntimeError(
            "Board graph runtime is not configured. The concrete Ladybug/Kuzu "
            "adapter must be supplied by the edition composition root through "
            "KGProviderRegistry.board_graph_runtime."
        )
    return runtime


def board_kuzu_path(board_id: str) -> Path:
    return _runtime().board_kuzu_path(board_id)


def bootstrap_board_graph(board_id: str) -> Any:
    return _runtime().bootstrap_board_graph(board_id)


def ensure_board_graph_bootstrapped(board_id: str) -> None:
    _runtime().ensure_board_graph_bootstrapped(board_id)


def open_board_connection(board_id: str) -> Any:
    return _runtime().open_board_connection(board_id)


def open_board_connection_raw(board_id: str) -> Any:
    return _runtime().open_board_connection_raw(board_id)


def close_all_connections(board_id: str | None = None) -> None:
    _runtime().close_all_connections(board_id)


def close_board_db_cache(board_id: str | None = None) -> None:
    _runtime().close_board_db_cache(board_id)


def purge_board_graph_storage(
    board_id: str,
    *,
    reason: str = "manual",
) -> list[str]:
    return _runtime().purge_board_graph_storage(board_id, reason=reason)


def migrate_schema_for_board(board_id: str) -> bool:
    return bool(_runtime().migrate_schema_for_board(board_id))


def reset_bootstrap_cache_for_tests() -> None:
    _runtime().reset_bootstrap_cache_for_tests()


def apply_ladybug_lifecycle_step(*args: Any, **kwargs: Any) -> Any:
    return _runtime().apply_ladybug_lifecycle_step(*args, **kwargs)


def load_vector_extension(conn: Any) -> None:
    _runtime().load_vector_extension(conn)


def _open_kuzu_db(path: Path) -> Any:
    return _runtime().open_kuzu_db(path)


def _is_ladybug_corruption_error(exc: BaseException) -> bool:
    return bool(_runtime().is_ladybug_corruption_error(exc))


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
    "purge_board_graph_storage",
    "relationship_endpoint_pairs",
    "reset_bootstrap_cache_for_tests",
    "resolve_relationship_endpoint_pair",
    "stable_rel_type_entries",
    "vector_index_name",
]
