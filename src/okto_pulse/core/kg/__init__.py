"""Knowledge Graph layer — Kùzu per-board graph + embedding provider (MVP Fase 0)."""

from okto_pulse.core.kg.embedding import (
    EmbeddingProvider,
    get_embedding_provider,
)
from okto_pulse.core.kg.schema import (
    EDGE_LAYERS,
    EDGE_METADATA_COLUMNS,
    NODE_TYPES,
    REL_TYPES,
    VECTOR_INDEX_TYPES,
    SCHEMA_VERSION,
    bootstrap_board_graph,
    board_kuzu_path,
    ensure_board_graph_bootstrapped,
    migrate_edge_metadata,
    open_board_connection,
    reset_bootstrap_cache_for_tests,
    vector_index_name,
)

__all__ = [
    "EmbeddingProvider",
    "get_embedding_provider",
    "EDGE_LAYERS",
    "EDGE_METADATA_COLUMNS",
    "NODE_TYPES",
    "REL_TYPES",
    "VECTOR_INDEX_TYPES",
    "SCHEMA_VERSION",
    "bootstrap_board_graph",
    "board_kuzu_path",
    "ensure_board_graph_bootstrapped",
    "migrate_edge_metadata",
    "open_board_connection",
    "reset_bootstrap_cache_for_tests",
    "vector_index_name",
]
