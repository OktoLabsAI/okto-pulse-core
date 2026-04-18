"""Kùzu per-board graph schema — 11 node tables, 10 rel tables, 5 vector indexes.

Idempotent bootstrap: `bootstrap_board_graph(board_id)` creates or opens the
per-board `.kuzu` directory under `kg_base_dir/boards/{board_id}/graph.kuzu`,
applies DDL, creates HNSW vector indexes for searchable node types, and
records the schema version on a Board meta node.
"""

from __future__ import annotations

import gc
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger("okto_pulse.kg.schema")

SCHEMA_VERSION = "0.2.0"

# Provenance metadata required on every rel (KG Pipeline v2 — spec c48a5c33).
# `layer` is a closed enum validated by the worker/agent and the layer_isolation
# business rule. `legacy` is reserved for rels migrated from v0.1.0 where no
# layer attribution is available; agents must treat legacy edges as lower-trust
# than fresh `deterministic` emissions.
EDGE_LAYERS: tuple[str, ...] = ("deterministic", "cognitive", "fallback", "legacy")

# Added in v0.2.0. Names match the add_edge_candidate payload contract so the
# worker can pass attrs through without remapping.
EDGE_METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("layer", "STRING"),
    ("rule_id", "STRING"),
    ("created_by", "STRING"),
    ("fallback_reason", "STRING"),
)

# 11 node types listed in the MVP Fase 0 spec (FR-N nodes).
NODE_TYPES: tuple[str, ...] = (
    "Decision",
    "Criterion",
    "Constraint",
    "Assumption",
    "Requirement",
    "Entity",
    "APIContract",
    "TestScenario",
    "Bug",
    "Learning",
    "Alternative",
)

# Node types that get HNSW vector indexes for semantic search.
# Entity/Criterion/Constraint join Decision and Learning because they carry
# semantic content that the tier primário queries (find_similar_decisions,
# find_contradictions, explain_constraint, get_learning_from_bugs).
VECTOR_INDEX_TYPES: tuple[str, ...] = (
    "Decision",
    "Criterion",
    "Constraint",
    "Entity",
    "Learning",
)

# 10 rel types. supersedes and contradicts are the two core semantic relations
# the tier primário walks variable-length paths on; the rest encode provenance
# (derives_from), context (relates_to, mentions), co-reference (depends_on),
# and quality feedback (violates, tests, implements, validates).
REL_TYPES: tuple[tuple[str, str, str], ...] = (
    # (rel_name, from_type, to_type)
    ("supersedes", "Decision", "Decision"),
    ("contradicts", "Decision", "Decision"),
    ("derives_from", "Decision", "Requirement"),
    ("relates_to", "Decision", "Alternative"),
    ("mentions", "Decision", "Entity"),
    ("depends_on", "Decision", "Decision"),
    ("violates", "Bug", "Constraint"),
    ("implements", "APIContract", "Requirement"),
    ("tests", "TestScenario", "Criterion"),
    ("validates", "Learning", "Bug"),
)

# Multi-pair rel types — Kuzu supports `CREATE REL TABLE x (FROM A TO B, FROM
# C TO D, ...)` and we leverage that for the hierarchy backbone so a single
# `belongs_to` rel name can connect any artifact-typed node to its parent
# Entity (Spec/Sprint/Card). Keeps the schema readable and queries terse:
#     MATCH (n)-[:belongs_to]->(p:Entity) RETURN n, p
# Without this we'd need 8 `belongs_to_<type>` variants polluting the schema.
MULTI_REL_TYPES: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
    ("belongs_to", (
        ("Entity", "Entity"),
        ("Requirement", "Entity"),
        ("Constraint", "Entity"),
        ("Criterion", "Entity"),
        ("TestScenario", "Entity"),
        ("APIContract", "Entity"),
        ("Decision", "Entity"),
        ("Bug", "Entity"),
        ("Alternative", "Entity"),
        ("Learning", "Entity"),
    )),
)

# Common attributes shared across every node type — written once in the DDL
# template below. Embedding is always declared even on types without a vector
# index so nothing breaks if a future tool queries similarity on them.
_COMMON_NODE_ATTRS = """
    id STRING PRIMARY KEY,
    title STRING,
    content STRING,
    context STRING,
    justification STRING,
    source_artifact_ref STRING,
    source_session_id STRING,
    created_at TIMESTAMP,
    created_by_agent STRING,
    source_confidence DOUBLE,
    validation_status STRING,
    corroboration_count INT64,
    superseded_by STRING,
    superseded_at TIMESTAMP,
    revocation_reason STRING,
    embedding DOUBLE[384]
""".strip()


@dataclass(frozen=True)
class BoardGraphHandle:
    """Handle returned by bootstrap_board_graph — path + schema version."""

    board_id: str
    path: Path
    schema_version: str


def _kg_base_dir() -> Path:
    """Resolve the KG base directory (defaults to ~/.okto-pulse)."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    raw = get_kg_registry().config.kg_base_dir
    return Path(os.path.expanduser(raw)).resolve()


def board_kuzu_path(board_id: str) -> Path:
    """Return the absolute path to a board's Kùzu graph directory."""
    if not board_id or "/" in board_id or ".." in board_id:
        raise ValueError(f"invalid board_id: {board_id!r}")
    return _kg_base_dir() / "boards" / board_id / "graph.kuzu"


def _build_node_ddl(node_type: str) -> str:
    return f"CREATE NODE TABLE IF NOT EXISTS {node_type} ({_COMMON_NODE_ATTRS})"


def _build_rel_ddl(rel_name: str, from_type: str, to_type: str) -> str:
    extra_cols = ", ".join(f"{name} {dtype}" for name, dtype in EDGE_METADATA_COLUMNS)
    return (
        f"CREATE REL TABLE IF NOT EXISTS {rel_name} "
        f"(FROM {from_type} TO {to_type}, "
        f"confidence DOUBLE, "
        f"created_by_session_id STRING, "
        f"created_at TIMESTAMP, "
        f"{extra_cols})"
    )


def _build_multi_rel_ddl(rel_name: str, pairs: tuple[tuple[str, str], ...]) -> str:
    """Build a single REL TABLE statement covering many (from, to) pairs.

    Kùzu accepts `CREATE REL TABLE x (FROM A TO B, FROM C TO D, ...)` to
    declare a multi-typed relationship — used by the hierarchy backbone so
    one `belongs_to` rel name connects every artifact type to its parent.
    """
    extra_cols = ", ".join(f"{name} {dtype}" for name, dtype in EDGE_METADATA_COLUMNS)
    pair_clauses = ", ".join(f"FROM {f} TO {t}" for f, t in pairs)
    return (
        f"CREATE REL TABLE IF NOT EXISTS {rel_name} "
        f"({pair_clauses}, "
        f"confidence DOUBLE, "
        f"created_by_session_id STRING, "
        f"created_at TIMESTAMP, "
        f"{extra_cols})"
    )


def _ensure_edge_metadata_columns(conn, rel_name: str) -> list[str]:
    """ALTER TABLE ADD for every v0.2.0 metadata column missing on `rel_name`.

    Returns the list of columns actually added. Idempotent: Kùzu raises on
    duplicate ADD so we catch-and-continue; no pre-check query is needed.
    """
    added: list[str] = []
    for col_name, col_type in EDGE_METADATA_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE {rel_name} ADD {col_name} {col_type}")
            added.append(col_name)
        except Exception:
            # Already exists or ALTER unsupported for this version — both safe.
            pass
    return added


def _backfill_legacy_edge_metadata(conn, rel_name: str) -> int:
    """Tag pre-v0.2.0 rels that still have NULL layer as `legacy`.

    Sets layer='legacy', rule_id='legacy_pre_v2', created_by='worker_legacy'
    only where the current value is NULL so re-running the migration is safe.
    Returns the number of rels updated (best-effort; Kùzu's UPDATE count isn't
    always exposed so we return 0 on opaque drivers).
    """
    try:
        conn.execute(
            f"MATCH ()-[r:{rel_name}]->() WHERE r.layer IS NULL "
            f"SET r.layer = 'legacy', "
            f"r.rule_id = coalesce(r.rule_id, 'legacy_pre_v2'), "
            f"r.created_by = coalesce(r.created_by, 'worker_legacy')"
        )
    except Exception as exc:
        logger.warning(
            "migrate_edge_metadata.backfill_failed rel=%s err=%s",
            rel_name, exc,
            extra={"event": "migrate_edge_metadata.backfill_failed",
                   "rel_name": rel_name},
        )
        return 0
    return 0


def migrate_edge_metadata(board_id: str) -> dict[str, Any]:
    """Apply the v0.1.0 → v0.2.0 edge metadata migration to a board.

    Idempotent — safe to call on every bootstrap. Adds missing columns on every
    rel table and backfills NULL `layer` as `legacy` so consumers that filter
    by layer don't drop historical edges unexpectedly. Callable manually via
    the CLI (`okto-pulse kg backfill --migrate-schema`) or from within
    bootstrap_board_graph.

    Returns a dict summary `{rel_name: [added_columns]}` useful for audit logs.
    """
    summary: dict[str, Any] = {}
    path = board_kuzu_path(board_id)
    if not path.exists():
        return summary

    # Release pool-cached handle first. On Windows Kùzu holds an exclusive
    # file lock per-process, so a cached connection from the pool would
    # collide with the fresh one we're about to open.
    close_all_connections(board_id)

    with open_board_connection(board_id) as (_db, conn):
        for rel_name, _from_type, _to_type in REL_TYPES:
            added = _ensure_edge_metadata_columns(conn, rel_name)
            _backfill_legacy_edge_metadata(conn, rel_name)
            summary[rel_name] = added

    logger.info(
        "migrate_edge_metadata.done board=%s summary=%s",
        board_id, summary,
        extra={"event": "migrate_edge_metadata.done", "board_id": board_id,
               "summary": summary},
    )
    return summary


def _board_meta_ddl() -> str:
    return (
        "CREATE NODE TABLE IF NOT EXISTS BoardMeta ("
        "board_id STRING PRIMARY KEY, "
        "schema_version STRING, "
        "bootstrapped_at TIMESTAMP"
        ")"
    )


def vector_index_name(node_type: str) -> str:
    """Canonical HNSW index name per node type."""
    return f"{node_type.lower()}_embedding_idx"


def load_vector_extension(conn) -> None:
    """Ensure the Kùzu VECTOR extension is loaded on the given connection.

    INSTALL is idempotent (persists in the DB file); LOAD must be called on
    every fresh connection but is also a no-op when already loaded.
    """
    try:
        conn.execute("INSTALL VECTOR")
    except Exception:
        pass  # already installed or bundled
    try:
        conn.execute("LOAD VECTOR")
    except Exception:
        pass  # already loaded or bundled


# Cache of boards already migrated this process — avoids re-running the
# write-heavy ALTER+UPDATE DDL on every connection open (which competes with
# concurrent commits for Kùzu's per-database lock and silently rolled back the
# real edge writes during historical drains).
_MIGRATED_BOARDS: set[str] = set()


def _board_needs_migration(board_id: str) -> bool:
    """Returns True iff the per-board Kùzu DB lacks a rel table the current
    schema expects. Cheap probe: open a short-lived connection and read the
    rel-table catalog; cache positive results so subsequent opens skip the
    check entirely."""
    if board_id in _MIGRATED_BOARDS:
        return False
    try:
        import kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        db = kuzu.Database(str(path))
        conn = kuzu.Connection(db)
        try:
            res = conn.execute("CALL show_tables() WHERE type='REL' RETURN name")
            existing = set()
            while res.has_next():
                existing.add(res.get_next()[0])
        finally:
            del conn, db
        expected = {r[0] for r in REL_TYPES} | {m[0] for m in MULTI_REL_TYPES}
        if expected.issubset(existing):
            _MIGRATED_BOARDS.add(board_id)
            return False
        return True
    except Exception:
        # Probe failed — assume migration is needed; the apply itself is
        # idempotent so a false positive only costs one extra DDL pass.
        return True


def _migrate_board_schema(board_id: str) -> None:
    """One-shot schema apply for a pre-existing board. Wraps the DDL pass
    in its own short-lived connection so the caller's connection lifecycle
    isn't tangled with the migration's, then caches the board as migrated."""
    try:
        import kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        db = kuzu.Database(str(path))
        conn = kuzu.Connection(db)
        try:
            apply_schema_to_connection(conn)
        finally:
            del conn, db
        _MIGRATED_BOARDS.add(board_id)
    except Exception as exc:
        logger.warning(
            "board_migrate.apply_failed board=%s err=%s",
            board_id, exc,
        )


def apply_schema_to_connection(conn) -> None:
    """Run all DDL against an already-open Kùzu connection.

    Every statement uses ``IF NOT EXISTS`` (or the equivalent try/except for
    ALTER ADD), so this is safe to invoke on every BoardConnection open. It's
    the migration path for boards bootstrapped under an older schema — the
    deterministic worker rolling out new rel tables (e.g. ``belongs_to``)
    relies on this re-running so existing boards don't need a destructive
    reset to pick up additions.
    """
    load_vector_extension(conn)
    conn.execute(_board_meta_ddl())
    for node_type in NODE_TYPES:
        conn.execute(_build_node_ddl(node_type))
    for rel_name, from_type, to_type in REL_TYPES:
        conn.execute(_build_rel_ddl(rel_name, from_type, to_type))
        # v0.1.0 → v0.2.0 backfill: ALTER ADD the metadata cols on legacy
        # tables and tag any pre-existing rows so queries filtering by
        # layer stay correct.
        _ensure_edge_metadata_columns(conn, rel_name)
        _backfill_legacy_edge_metadata(conn, rel_name)

    # Multi-pair rel types (hierarchy backbone — `belongs_to`).
    for rel_name, pairs in MULTI_REL_TYPES:
        conn.execute(_build_multi_rel_ddl(rel_name, pairs))
        _ensure_edge_metadata_columns(conn, rel_name)
        _backfill_legacy_edge_metadata(conn, rel_name)


def bootstrap_board_graph(board_id: str) -> BoardGraphHandle:
    """Create or open a per-board Kùzu graph with the full MVP schema.

    Idempotent: re-invoking returns the same handle without re-creating tables.
    """
    try:
        import kuzu  # type: ignore
    except ImportError as exc:  # pragma: no cover — deps required for runtime
        raise RuntimeError(
            "kuzu is required for the knowledge graph layer — "
            "install with `pip install kuzu`"
        ) from exc

    path = board_kuzu_path(board_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    db = kuzu.Database(str(path))
    conn = kuzu.Connection(db)
    try:
        apply_schema_to_connection(conn)

        # Vector indexes: one HNSW index per searchable node type. Kùzu 0.11
        # CREATE_VECTOR_INDEX takes (table, idx_name, col_name) positional +
        # named metric. We declare `cosine` explicitly so the `1 - distance`
        # conversion in search.py stays correct even if Kùzu's default metric
        # changes across versions.
        for node_type in VECTOR_INDEX_TYPES:
            idx = vector_index_name(node_type)
            try:
                conn.execute(
                    f"CALL CREATE_VECTOR_INDEX("
                    f"'{node_type}', '{idx}', 'embedding', "
                    f"metric := 'cosine')"
                )
            except Exception:
                # Index exists already — fine.
                pass

        # Record schema version on the BoardMeta singleton. Use DELETE+CREATE
        # so a re-bootstrap updates the version if the schema has evolved.
        conn.execute(
            "MATCH (m:BoardMeta {board_id: $bid}) DELETE m",
            {"bid": board_id},
        )
        conn.execute(
            "CREATE (m:BoardMeta {board_id: $bid, schema_version: $v, "
            "bootstrapped_at: timestamp($ts)})",
            {
                "bid": board_id,
                "v": SCHEMA_VERSION,
                "ts": _now_iso(),
            },
        )
    finally:
        del conn
        del db

    _MIGRATED_BOARDS.add(board_id)

    return BoardGraphHandle(
        board_id=board_id,
        path=path,
        schema_version=SCHEMA_VERSION,
    )


class BoardConnection:
    """Context manager for a per-board Kùzu connection.

    Preferred usage:
        with BoardConnection(board_id) as (db, conn):
            conn.execute(...)

    On ``__exit__`` (or explicit ``.close()``) the connection is released via
    ``conn.close()`` when available, followed by ``del`` and ``gc.collect()``.
    The gc call is mandatory on Windows — Kùzu holds an exclusive file lock
    on the ``.kuzu`` directory for the lifetime of the Python-side handle,
    and without a gc pass the lock can outlive the ``del`` and block any
    subsequent rmtree/bootstrap on the same board.

    Exceptions raised by ``conn.close()`` are swallowed with a warning log:
    the caller is already unwinding and a close failure should not mask the
    original error or prevent the file lock from being released.

    Also iterable — ``db, conn = BoardConnection(bid)`` works as a drop-in
    for the legacy ``open_board_connection`` tuple signature during the
    retrofit. Legacy callers must ``del`` both handles themselves to release
    the lock; prefer the ``with`` form in new code.
    """

    def __init__(self, board_id: str) -> None:
        import kuzu  # type: ignore

        path = board_kuzu_path(board_id)
        first_open = not path.exists()
        if first_open:
            # Brand-new board — full bootstrap (creates path, vector indexes,
            # BoardMeta singleton).
            bootstrap_board_graph(board_id)
        elif _board_needs_migration(board_id):
            # Pre-existing board missing a rel table (e.g. `belongs_to` added
            # post-bootstrap). Run schema apply ONCE to backfill, then mark the
            # board as migrated so subsequent opens skip the (write-heavy) DDL
            # pass — running it on every connection caused silent rollbacks of
            # parallel commits via Kùzu's lock manager.
            _migrate_board_schema(board_id)

        self._board_id = board_id
        self._db = kuzu.Database(str(path))
        self._conn = kuzu.Connection(self._db)
        load_vector_extension(self._conn)
        self._closed = False

    def __enter__(self):
        return self._db, self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __iter__(self):
        # Tuple unpacking transfers ownership to the caller (matches the
        # legacy `db, conn = open_board_connection(bid)` contract — caller
        # is responsible for `del conn, db` to release the Kùzu file lock).
        # Suppress close() on this wrapper so the temporary instance going
        # out of scope doesn't invalidate the handles we just yielded.
        self._closed = True
        yield self._db
        yield self._conn

    @property
    def db(self):
        return self._db

    @property
    def conn(self):
        return self._conn

    def close(self) -> None:
        """Release the Kùzu connection + file lock. Idempotent and best-effort."""
        if self._closed:
            return
        self._closed = True
        conn = getattr(self, "_conn", None)
        if conn is not None and hasattr(conn, "close"):
            try:
                conn.close()
            except Exception as exc:
                logger.warning(
                    "board_connection.close_failed board=%s err=%s",
                    self._board_id, exc,
                    extra={
                        "event": "board_connection.close_failed",
                        "board_id": self._board_id,
                    },
                )
        try:
            del self._conn
        except Exception:
            pass
        try:
            del self._db
        except Exception:
            pass
        gc.collect()

def open_board_connection(board_id: str) -> BoardConnection:
    """Open a fresh Kùzu connection for a board as a :class:`BoardConnection`.

    Returns a :class:`BoardConnection` — use as a context manager
    (``with open_board_connection(bid) as (db, conn):``) to guarantee
    ``close()`` runs even under exceptions. The return value is also
    iterable, so legacy ``db, conn = open_board_connection(bid)`` sites
    continue to work during the retrofit.
    """
    return BoardConnection(board_id)


def open_board_connection_raw(board_id: str):
    """Deprecated: use ``with open_board_connection(bid) as (db, conn):``.

    Returns ``(db, conn)`` as a plain tuple — no context manager wrapping.
    The caller is responsible for ``del conn, db`` (and ideally a follow-up
    ``gc.collect()`` on Windows) to release the Kùzu file lock.

    Exists so legacy call sites can be migrated incrementally across
    several PRs without forcing a single-commit flag-day rewrite.
    """
    import warnings

    warnings.warn(
        "open_board_connection_raw is deprecated; use "
        "`with open_board_connection(board_id) as (db, conn):` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return tuple(BoardConnection(board_id))


def close_all_connections(board_id: str | None = None) -> None:
    """Release Kùzu connections so the underlying ``.kuzu`` dirs can be rmtree'd.

    ``board_id=None``: close the global discovery singleton *and* every
    per-board connection the pool is holding.

    ``board_id=<id>``: close only that board's pooled connection. The global
    singleton is left alone because it points at a different ``.kuzu`` dir.

    Idempotent and best-effort: missing pool (card 1.2 not yet landed) or an
    already-closed global are both no-ops. The primary consumer is the
    right-to-erasure path, which needs every handle released before the
    rmtree runs on Windows.
    """
    try:
        from okto_pulse.core.kg.connection_pool import (  # type: ignore
            close_board_connection,
            close_all_board_connections,
        )
    except ImportError:
        close_board_connection = None  # type: ignore[assignment]
        close_all_board_connections = None  # type: ignore[assignment]

    if board_id is not None:
        if close_board_connection is not None:
            try:
                close_board_connection(board_id)
            except Exception as exc:
                logger.warning(
                    "close_all.board_failed board=%s err=%s", board_id, exc,
                    extra={
                        "event": "close_all.board_failed",
                        "board_id": board_id,
                    },
                )
        return

    if close_all_board_connections is not None:
        try:
            close_all_board_connections()
        except Exception as exc:
            logger.warning(
                "close_all.pool_failed err=%s", exc,
                extra={"event": "close_all.pool_failed"},
            )

    # global is released only when closing everything — per-board callers
    # (e.g. single-board DELETE) must not nuke the shared discovery handle.
    from okto_pulse.core.kg.global_discovery.schema import close_global_connection

    close_global_connection()


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
