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

GLOBAL_SCHEMA_VERSION = "0.1.1"
GLOBAL_DISCOVERY_FILENAME = "discovery.lbug"
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
    """Add and backfill graph_layer on existing global DecisionDigest rows."""
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
            "SET d.graph_layer = 'canonical'"
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


def _global_quarantine_service():
    """Build the canonical KGQuarantineService for global discovery purges.

    scope_roots is the global discovery storage dir. quarantine base_dir
    lives one level up so it's siblings to per-board quarantine.
    """
    from okto_pulse.core.kg.quarantine import KGQuarantineService

    global_dir = _global_kuzu_path().parent
    quarantine_base = global_dir.parent  # one level up
    return KGQuarantineService(
        base_dir=quarantine_base,
        scope_roots=[global_dir],
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
    """Quarantine-then-clear the local global LadybugDB discovery file and sidecars.

    KG-01.4 (val_79e6f555 rework): purges of ``discovery.lbug`` and
    sidecars MUST go through ``KGQuarantineService`` first. If
    quarantine fails the whole purge is aborted with the evidence
    preserved at the original path (per FR7 / AC10 / IR ir_f175bc42).

    KG-01.3.1 boundary (val_441ad311): destructive global write path;
    requires an active ``under_global_safe_write`` guard. In SOFT mode
    (default) a missing guard logs + bumps the counter; in STRICT
    raises ``WriteLifecycleViolation`` BEFORE any filesystem mutation.
    """
    from okto_pulse.core.kg.quarantine import QuarantineError
    from okto_pulse.core.kg.write_barrier import require_global_write_token

    require_global_write_token()

    path = _global_kuzu_path()
    close_global_connection()
    targets: list[Path] = []
    if path.exists():
        targets.append(path)
    if path.parent.exists():
        targets.extend(sorted(path.parent.glob(path.name + ".*")))

    if not targets:
        return []

    service = _global_quarantine_service()
    try:
        response = service.create(
            board_id="_global",
            graph_type="global_discovery",
            affected_paths=[str(t) for t in targets],
            reason=reason,
            correlation_ids=[],
        )
    except QuarantineError as exc:
        logger.error(
            "global_discovery.purge_blocked_quarantine_failed "
            "reason=%s code=%s err=%s",
            reason, exc.code.value, exc.reason,
            extra={
                "event": "global_discovery.purge_blocked_quarantine_failed",
                "reason": reason,
                "code": exc.code.value,
            },
        )
        return []

    moved_count = response.files_moved
    removed_str = [str(t) for t in targets[:moved_count]]
    logger.warning(
        "global_discovery.purged reason=%s removed=%d "
        "quarantine_id=%s manifest=%s",
        reason, moved_count,
        response.quarantine_id, response.manifest_ref,
        extra={
            "event": "global_discovery.purged",
            "reason": reason,
            "quarantine_id": response.quarantine_id,
            "manifest_ref": response.manifest_ref,
            "files_moved": moved_count,
        },
    )
    return removed_str


def bootstrap_global_discovery() -> Path:
    """Create or open the global discovery Kuzu meta-graph. Idempotent.

    KG-01.3.1 boundary (val_441ad311 rework): this path runs DDL against
    `discovery.lbug` and may invoke ``purge_global_discovery_storage`` on
    corruption. It MUST be wrapped in ``under_global_safe_write`` by the
    caller. The barrier check raises ``WriteLifecycleViolation`` in
    STRICT mode and logs+bumps ``kg_unguarded_write_total`` in SOFT.
    """
    from okto_pulse.core.kg.write_barrier import require_global_write_token

    require_global_write_token()

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
        _raise_existing_global_graph_open_failed(
            path=path,
            operation="bootstrap",
            exc=exc,
        )
    conn = kuzu.Connection(db)
    try:
        load_vector_extension(conn)
        for ddl in NODE_DDL:
            conn.execute(ddl)
        for ddl in REL_DDL:
            conn.execute(ddl)
        _ensure_decision_digest_layer_column(conn)
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


def ensure_global_discovery_layer_schema() -> list[str]:
    """Ensure existing global discovery graphs expose DecisionDigest.graph_layer."""
    from okto_pulse.core.kg.write_barrier import require_global_write_token

    require_global_write_token()
    _gdb, conn = open_global_connection()
    try:
        return _ensure_decision_digest_layer_column(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
            _raise_existing_global_graph_open_failed(
                path=path,
                operation="open_connection",
                exc=exc,
            )
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
