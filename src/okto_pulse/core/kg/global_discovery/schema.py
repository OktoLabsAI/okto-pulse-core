"""Global discovery meta-graph schema definitions.

4 node tables: Board, Topic, Entity, DecisionDigest
7 rel tables: HAS_TOPIC, MENTIONS_ENTITY, CONTAINS_DECISION,
             TOPIC_RELATES_TO, ENTITY_RELATES_TO, DECISION_MENTIONS_ENTITY,
             DECISION_DERIVES_FROM
4 HNSW vector indexes: Board.summary_embedding, Topic.centroid_embedding,
                       Entity.embedding, DecisionDigest.embedding (cosine 384-dim)
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("okto_pulse.kg.global_discovery.schema")

GLOBAL_SCHEMA_VERSION = "0.1.1"
DECISION_DIGEST_GRAPH_LAYER_COLUMN = ("graph_layer", "STRING")

NODE_DDL = [
    """CREATE NODE TABLE IF NOT EXISTS Board (
        board_id STRING PRIMARY KEY,
        name STRING,
        summary STRING,
        summary_embedding DOUBLE[384],
        topic_count INT64,
        entity_count INT64,
        decision_count INT64,
        last_sync_at TIMESTAMP
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Topic (
        id STRING PRIMARY KEY,
        name STRING,
        centroid_embedding DOUBLE[384],
        member_count INT64,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Entity (
        id STRING PRIMARY KEY,
        canonical_name STRING,
        aliases STRING,
        embedding DOUBLE[384],
        mention_count INT64,
        last_seen TIMESTAMP
    )""",
    """CREATE NODE TABLE IF NOT EXISTS DecisionDigest (
        id STRING PRIMARY KEY,
        board_id STRING,
        original_node_id STRING,
        title STRING,
        one_line_summary STRING,
        node_type STRING,
        graph_layer STRING,
        embedding DOUBLE[384],
        created_at TIMESTAMP
    )""",
]


def _is_duplicate_column_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "already exists" in msg
        or "duplicate" in msg
        or "column with name" in msg
    )


def _table_column_names(conn, table_name: str) -> set[str]:
    res = None
    names: set[str] = set()
    try:
        res = conn.execute(f"CALL TABLE_INFO('{table_name}') RETURN *")
        while res.has_next():
            row = res.get_next()
            for cell in row:
                if isinstance(cell, str):
                    names.add(cell)
                    break
    finally:
        if res is not None:
            try:
                res.close()
            except Exception:
                pass
    return names


def _ensure_decision_digest_layer_column(conn) -> list[str]:
    """Add ``graph_layer`` and fail-CLOSED backfill legacy DecisionDigest rows.

    R1-IMP3 / FR5: a legacy digest that predates the ``graph_layer`` column has
    NULL after the ALTER. It must NOT be defaulted to ``canonical`` (that would
    leak an unverified legacy fact into canonical-only discovery). Instead it is
    stamped ``legacy_unknown`` — outside canonical until the R1-IMP1 parity
    reconciler maps it to the correct ``expected_digest_layer`` from the board
    graph. Identity (``original_node_id``/``source_artifact_ref``) is untouched.
    """
    col_name, col_type = DECISION_DIGEST_GRAPH_LAYER_COLUMN
    added: list[str] = []
    try:
        columns = _table_column_names(conn, "DecisionDigest")
    except Exception:
        columns = set()
    if col_name not in columns:
        try:
            conn.execute(f"ALTER TABLE DecisionDigest ADD {col_name} {col_type}")
            added.append(col_name)
        except Exception as exc:
            if not _is_duplicate_column_error(exc):
                raise
    try:
        conn.execute(
            "MATCH (d:DecisionDigest) "
            "WHERE d.graph_layer IS NULL "
            "SET d.graph_layer = 'legacy_unknown'"
        )
    except Exception as exc:
        logger.debug(
            "global_discovery.layer_backfill_skipped err=%s",
            exc,
            extra={"event": "global_discovery.layer_backfill_skipped"},
        )
    return added

REL_DDL = [
    "CREATE REL TABLE IF NOT EXISTS HAS_TOPIC (FROM Board TO Topic)",
    "CREATE REL TABLE IF NOT EXISTS MENTIONS_ENTITY (FROM Board TO Entity)",
    "CREATE REL TABLE IF NOT EXISTS CONTAINS_DECISION (FROM Board TO DecisionDigest)",
    "CREATE REL TABLE IF NOT EXISTS TOPIC_RELATES_TO (FROM Topic TO Topic, weight DOUBLE)",
    "CREATE REL TABLE IF NOT EXISTS ENTITY_RELATES_TO (FROM Entity TO Entity, weight DOUBLE)",
    "CREATE REL TABLE IF NOT EXISTS DECISION_MENTIONS_ENTITY (FROM DecisionDigest TO Entity)",
    "CREATE REL TABLE IF NOT EXISTS DECISION_DERIVES_FROM (FROM DecisionDigest TO DecisionDigest)",
]

VECTOR_INDEXES = [
    ("Board", "board_summary_idx", "summary_embedding"),
    ("Topic", "topic_centroid_idx", "centroid_embedding"),
    ("Entity", "entity_embedding_idx", "embedding"),
    ("DecisionDigest", "digest_embedding_idx", "embedding"),
]


def _global_runtime():
    from okto_pulse.core.kg.interfaces import get_kg_registry

    return get_kg_registry().require_global_discovery_runtime()


def _is_ladybug_corruption_error(exc: BaseException) -> bool:
    try:
        runtime = getattr(_global_runtime(), "is_ladybug_corruption_error", None)
        if runtime is not None:
            return bool(runtime(exc))
    except Exception:
        pass
    msg = str(exc).lower()
    return (
        "corrupted wal file" in msg
        or "invalid wal record" in msg
        or "not a valid lbug database file" in msg
    )


def _raise_existing_global_graph_open_failed(
    *,
    path: Path,
    operation: str,
    exc: BaseException,
) -> None:
    """Fail closed when global discovery storage already exists but fails open.

    On-demand bootstrap may create a missing discovery graph, but it must not
    quarantine an existing one just because the probe hit a WAL/open failure.
    That recovery decision belongs to an explicit operator action.
    """
    logger.error(
        "global_discovery.existing_graph_open_failed_preserved "
        "operation=%s path=%s err=%s",
        operation, path, exc,
        extra={
            "event": "global_discovery.existing_graph_open_failed_preserved",
            "operation": operation,
            "path": str(path),
            "error": str(exc),
        },
    )
    raise RuntimeError(
        "Existing global discovery LadybugDB graph could not be opened during "
        f"{operation}; refusing to auto-bootstrap or purge it. path={path}. "
        "Use the explicit KG Health recovery flow after reviewing the "
        "rebuild/quarantine report; the current files were preserved."
    ) from exc


def purge_global_discovery_storage(*, reason: str = "manual") -> list[str]:
    """Delegate destructive global discovery purge to the edition runtime."""
    return _global_runtime().purge(reason=reason)


def bootstrap_global_discovery() -> Path:
    """Create/open the edition-owned global discovery graph. Idempotent."""
    return _global_runtime().bootstrap()


def ensure_global_discovery_layer_schema() -> list[str]:
    """Ensure existing global discovery graphs expose DecisionDigest.graph_layer."""
    return _global_runtime().ensure_layer_schema()


def open_global_connection():
    """Open a connection through the edition-owned Global Discovery runtime."""
    return _global_runtime().open_connection()


def close_global_connection() -> None:
    """Close the edition-owned Global Discovery runtime handle."""
    _global_runtime().close()


def global_discovery_graph_path() -> Path:
    """Return the edition-owned global discovery graph path."""
    return _global_runtime().global_graph_path()


def reset_global_discovery_runtime_for_tests() -> None:
    """Reset the edition-owned Global Discovery runtime handle."""
    try:
        runtime = _global_runtime()
    except RuntimeError:
        return
    runtime.reset_for_tests()
