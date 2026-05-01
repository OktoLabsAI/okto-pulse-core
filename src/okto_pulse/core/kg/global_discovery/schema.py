"""Global discovery LadybugDB meta-graph schema.

Path: ~/.okto-pulse/global/discovery.lbug
4 node tables: Board, Topic, Entity, DecisionDigest
7 rel tables: HAS_TOPIC, MENTIONS_ENTITY, CONTAINS_DECISION,
             TOPIC_RELATES_TO, ENTITY_RELATES_TO, DECISION_MENTIONS_ENTITY,
             DECISION_DERIVES_FROM
4 HNSW vector indexes: Board.summary_embedding, Topic.centroid_embedding,
                       Entity.embedding, DecisionDigest.embedding (cosine 384-dim)
"""

from __future__ import annotations

import gc
import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger("okto_pulse.kg.global_discovery.schema")

GLOBAL_SCHEMA_VERSION = "0.1.0"
GLOBAL_DISCOVERY_FILENAME = "discovery.lbug"

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
        embedding DOUBLE[384],
        created_at TIMESTAMP
    )""",
]

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


def _global_kuzu_path() -> Path:
    from okto_pulse.core.infra.config import get_settings
    base = Path(os.path.expanduser(get_settings().kg_base_dir)).resolve()
    return base / "global" / GLOBAL_DISCOVERY_FILENAME


def _is_ladybug_corruption_error(exc: BaseException) -> bool:
    try:
        from okto_pulse.core.kg.schema import _is_ladybug_corruption_error as _is_corrupt
        return _is_corrupt(exc)
    except Exception:
        msg = str(exc).lower()
        return (
            "corrupted wal file" in msg
            or "invalid wal record" in msg
            or "not a valid lbug database file" in msg
        )


def purge_global_discovery_storage(*, reason: str = "manual") -> list[str]:
    """Delete the local global LadybugDB discovery file and sidecars.

    This only touches ``discovery.lbug`` plus ``discovery.lbug.*`` under the
    configured KG global directory. SQLite application state and per-board
    graph files are left intact; the next bootstrap recreates this meta-graph
    from pending/outbox activity.
    """
    path = _global_kuzu_path()
    close_global_connection()
    removed: list[str] = []
    targets: list[Path] = []
    if path.exists():
        targets.append(path)
    if path.parent.exists():
        targets.extend(sorted(path.parent.glob(path.name + ".*")))

    for target in targets:
        try:
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
            else:
                target.unlink(missing_ok=True)
            removed.append(str(target))
        except Exception as exc:
            logger.warning(
                "global_discovery.purge_failed path=%s reason=%s err=%s",
                target, reason, exc,
                extra={
                    "event": "global_discovery.purge_failed",
                    "path": str(target),
                    "reason": reason,
                },
            )

    if removed:
        logger.warning(
            "global_discovery.purged reason=%s removed=%d",
            reason, len(removed),
            extra={
                "event": "global_discovery.purged",
                "reason": reason,
                "removed": removed,
            },
        )
    return removed


def bootstrap_global_discovery() -> Path:
    """Create or open the global discovery Kuzu meta-graph. Idempotent."""
    try:
        import ladybug as kuzu
    except ImportError as exc:
        raise RuntimeError("kuzu required") from exc

    from okto_pulse.core.kg.schema import _open_kuzu_db, load_vector_extension

    path = _global_kuzu_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        db = _open_kuzu_db(path)
    except Exception as exc:
        if not _is_ladybug_corruption_error(exc):
            raise
        purge_global_discovery_storage(reason="corrupt_open_during_bootstrap")
        path.parent.mkdir(parents=True, exist_ok=True)
        db = _open_kuzu_db(path)
    conn = kuzu.Connection(db)
    try:
        load_vector_extension(conn)
        for ddl in NODE_DDL:
            conn.execute(ddl)
        for ddl in REL_DDL:
            conn.execute(ddl)
        for table, idx_name, col in VECTOR_INDEXES:
            try:
                conn.execute(
                    f"CALL CREATE_VECTOR_INDEX("
                    f"'{table}', '{idx_name}', '{col}', "
                    f"metric := 'cosine')"
                )
            except Exception:
                pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            db.close()
        except Exception:
            pass
        # E2E spec c2115d15 — TS-E follow-up. No Windows o Kùzu segura lock
        # OS-level via Database C++ enquanto o objeto Python existir. Forçar
        # gc.collect() libera o handle imediatamente para evitar lock
        # contention quando bootstrap é seguido de open_global_connection
        # no mesmo processo (race observado no log do servidor).
        del db
        gc.collect()
    return path


_global_db = None


def open_global_connection():
    """Open a connection to the global discovery Kuzu. Bootstrap on-demand.

    Returns (db, conn). The Database is cached as a module-level singleton
    to avoid Kuzu file-lock conflicts from multiple Database instances
    pointing at the same path.
    """
    global _global_db
    import ladybug as kuzu
    from okto_pulse.core.kg.schema import _open_kuzu_db, load_vector_extension

    path = _global_kuzu_path()
    if not path.exists():
        bootstrap_global_discovery()

    if _global_db is None:
        try:
            _global_db = _open_kuzu_db(path)
        except Exception as exc:
            if not _is_ladybug_corruption_error(exc):
                raise
            purge_global_discovery_storage(reason="corrupt_open_global_connection")
            bootstrap_global_discovery()
            _global_db = _open_kuzu_db(path)
    conn = kuzu.Connection(_global_db)
    load_vector_extension(conn)
    return _global_db, conn


def close_global_connection() -> None:
    """Close the cached global discovery ``_global_db`` and release its file lock.

    Idempotent: returns immediately if no Database is cached. Exceptions raised
    by the underlying ``close()`` are logged as warnings and not propagated —
    the caller is usually about to rmtree or re-bootstrap and a close failure
    should not block that path.

    ``gc.collect()`` is mandatory on Windows: Kùzu holds an OS-level lock on
    the ``discovery.kuzu`` directory for as long as the C++ Database object
    exists. Without the gc pass, the object can survive the ``del`` long
    enough for the next ``rmtree`` to fail with ``WinError 32``.
    """
    global _global_db
    db = _global_db
    if db is None:
        return
    _global_db = None
    if hasattr(db, "close"):
        try:
            db.close()
        except Exception as exc:
            logger.warning(
                "global_connection.close_failed err=%s", exc,
                extra={"event": "global_connection.close_failed"},
            )
    del db
    gc.collect()


def reset_global_db_for_tests() -> None:
    """Drop the cached global Database — forces re-open on next call.

    Thin wrapper around :func:`close_global_connection` for legacy test code.
    """
    close_global_connection()
