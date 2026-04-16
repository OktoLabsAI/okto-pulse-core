"""Global Discovery Layer — Kuzu meta-graph for cross-board search.

~/.okto-pulse/global/discovery.kuzu stores board summaries, topic clusters,
canonical entities, and decision digests with HNSW embeddings. Content-free
by design — only titles, summaries, and pointers. ACL-filtered at query time.
"""

from okto_pulse.core.kg.global_discovery.schema import (
    bootstrap_global_discovery,
    open_global_connection,
    reset_global_db_for_tests,
    GLOBAL_SCHEMA_VERSION,
)

__all__ = [
    "bootstrap_global_discovery",
    "open_global_connection",
    "reset_global_db_for_tests",
    "GLOBAL_SCHEMA_VERSION",
]
