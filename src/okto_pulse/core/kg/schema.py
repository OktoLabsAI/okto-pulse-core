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
import threading
import time
from dataclasses import dataclass
from pathlib import Path
import ladybug as kuzu  # type: ignore
from typing import Any

logger = logging.getLogger("okto_pulse.kg.schema")

SCHEMA_VERSION = "0.3.3"

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
#
# v0.3.0 replaced the binary validation_status + corroboration_count pair with
# a continuous `relevance_score` plus usage telemetry (query_hits,
# last_queried_at). See `docs/migrations/v0.3.0.md` for the rationale and the
# R2 scoring pipeline that consumes these fields.
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
    relevance_score DOUBLE,
    query_hits INT64,
    last_queried_at STRING,
    last_recomputed_at STRING,
    priority_boost DOUBLE,
    superseded_by STRING,
    superseded_at TIMESTAMP,
    revocation_reason STRING,
    human_curated BOOLEAN,
    embedding DOUBLE[384]
""".strip()

# Columns added in v0.3.0 — used by the migration probe and ALTER TABLE path
# when the node table already exists but lacks the new columns. Kùzu accepts
# ALTER TABLE ADD for nullable columns without DEFAULT, which is enough since
# primitives.py always supplies values at insert time.
RELEVANCE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("relevance_score", "DOUBLE"),
    ("query_hits", "INT64"),
    ("last_queried_at", "STRING"),
)

# Columns added in v0.3.1 (spec 0eb51d3e — priority boost): priority_boost
# carries the additive score derived from card.priority at extraction time
# and is frozen on the node forever after. _resolve_priority_boost in
# scoring.py owns the mapping table.
PRIORITY_BOOST_COLUMNS: tuple[tuple[str, str], ...] = (
    ("priority_boost", "DOUBLE"),
)

# Columns added in v0.3.2 (spec 4007e4a3 — Ideação #3 manual edit
# preservation): human_curated marks nodes that a human curator has edited
# directly via back-office. The UPDATE path in primitives skips writes to
# nodes with human_curated=TRUE unless the agent passes an explicit
# override in commit_overrides. NULL is treated as FALSE for retrocompat.
HUMAN_CURATED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("human_curated", "BOOLEAN"),
)

# Columns added in v0.3.3 (spec 28583299 — Ideação #4 relevance scoring
# dinâmico, BR8 + FR19): last_recomputed_at is the ISO timestamp of the
# last time the relevance_score was persisted. The daily decay tick reads
# the global oldest value to size its workload, and kg_health surfaces
# the freshness of the score for observability. Stored as STRING (same
# convention as last_queried_at) so legacy boards backfill cleanly.
LAST_RECOMPUTED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("last_recomputed_at", "STRING"),
)

# Columns removed in v0.3.0. Kùzu v0.6 has no ALTER TABLE DROP COLUMN, so the
# migration strategy is dump→drop→create→bulk-insert when these are detected.
LEGACY_NODE_COLUMNS: tuple[str, ...] = ("validation_status", "corroboration_count")


@dataclass(frozen=True)
class BoardGraphHandle:
    """Handle returned by bootstrap_board_graph — path + schema version."""

    board_id: str
    path: Path
    schema_version: str


# Bug d0f6bab2: process-wide cache of kuzu.Database per board path.
# Kùzu locks the .kuzu directory at the OS level while ANY Database object
# exists; spawning a new one for each BoardConnection guarantees lock
# contention as soon as two coroutines/threads touch the same board.
# Multiple kuzu.Connection instances over a single Database are safe and
# the supported pattern. The cache is freed by close_board_db_cache /
# close_all_connections (the rmtree paths).
_board_db_cache: dict[str, Any] = {}
_board_db_cache_lock = threading.Lock()


def _open_kuzu_db_path_cached(path: Path) -> Any:
    """Return a singleton kuzu.Database for ``path``, opening on miss.

    Lookup keyed by ``str(path)`` so resolved-vs-symlink callers converge.
    Open is serialized through a module lock so two concurrent misses do
    not double-create (which would itself trigger the lock contention we
    are trying to avoid). Used by every per-board callsite (BoardConnection
    + bootstrap/migration probes) to guarantee a single OS-level lock per
    board path within the process.
    """
    key = str(path)
    cached = _board_db_cache.get(key)
    if cached is not None:
        return cached
    with _board_db_cache_lock:
        cached = _board_db_cache.get(key)
        if cached is not None:
            return cached
        # Cache miss: call the raw factory directly to avoid recursion.
        db = _open_kuzu_db(path)
        _board_db_cache[key] = db
        logger.debug(
            "[KG] _board_db_cache.miss path=%s size=%d",
            path, len(_board_db_cache),
        )
        return db


def _open_kuzu_db_cached(board_id: str, path: Path) -> Any:
    """Backwards-compat shim — delegates to ``_open_kuzu_db_path_cached``."""
    return _open_kuzu_db_path_cached(path)


def close_board_db_cache(board_id: str | None = None) -> None:
    """Drop the cached Database(s) so the .kuzu dir can be rmtree'd or re-opened.

    ``board_id=None`` closes every cached Database (rmtree everything).
    Specific board: only that one. Idempotent — already-evicted is a no-op.
    """
    with _board_db_cache_lock:
        if board_id is None:
            keys = list(_board_db_cache.keys())
        else:
            target = str(board_kuzu_path(board_id))
            keys = [target] if target in _board_db_cache else []
        for key in keys:
            db = _board_db_cache.pop(key, None)
            if db is None:
                continue
            try:
                db.close()
            except Exception as exc:
                logger.warning(
                    "kg.db_cache.close_failed key=%s err=%s", key, exc,
                    extra={"event": "kg.db_cache.close_failed", "key": key},
                )
            del db
        if keys:
            gc.collect()  # Windows: ensure C++ handles release before next caller


class BoardConnection:
    """Context-managed Kùzu per-board database connection.

    Reuses a process-wide cached :class:`kuzu.Database` (Bug d0f6bab2) and
    opens a fresh :class:`kuzu.Connection` per instance — multiple
    connections over one Database is the supported pattern and avoids
    OS-level file lock contention between concurrent workers.

    Use as a context manager::

        with BoardConnection(board_id) as (db, conn):
            conn.execute("MATCH (m:BoardMeta) RETURN count(m)")

    ``close()`` releases the Connection only; the cached Database survives
    for the next caller. Use :func:`close_board_db_cache` (or the
    higher-level :func:`close_all_connections`) when the .kuzu dir itself
    must be released — e.g. before ``rmtree`` or schema migration.
    """

    def __init__(self, board_id: str) -> None:
        self._board_id = board_id
        self._closed = False
        # Defensive: self-heal missing or partial graphs before we open our
        # own handle. No-op on hot boards (cache hit in
        # ensure_board_graph_bootstrapped).
        ensure_board_graph_bootstrapped(board_id)
        path = board_kuzu_path(board_id)
        logger.debug("[KG] BoardConnection.__init__ board_id=%s path=%s", board_id, path)
        self.db = _open_kuzu_db_cached(board_id, path)
        logger.debug("[KG] Kùzu database (cached) for board_id=%s", board_id)
        self.conn = kuzu.Connection(self.db)  # type: ignore[attr-defined]
        logger.debug("[KG] Kùzu connection created successfully for board_id=%s", board_id)

    def __enter__(self) -> tuple[Any, Any]:
        return self.db, self.conn

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __iter__(self) -> Any:
        """Yield (db, conn) so ``tuple(BoardConnection(bid))`` works."""
        yield self.db
        yield self.conn

    def close(self) -> None:
        """Close the connection only; the cached Database survives.

        Idempotent. To release the OS file lock and allow a rmtree, call
        :func:`close_board_db_cache` (or the higher-level
        :func:`close_all_connections`) instead.
        """
        if self._closed:
            return
        logger.debug("[KG] BoardConnection.close board_id=%s", self._board_id)
        self._closed = True
        try:
            del self.conn
        except Exception:
            pass
        # NOTE: do NOT close self.db — the Database is shared via the
        # process-wide cache (Bug d0f6bab2). Releasing it here would yank
        # the lock from concurrent BoardConnections operating on the same
        # board.


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


def _open_kuzu_db(path: Path):
    """Single factory for every ``kuzu.Database()`` call in the core.

    Reads ``kg_kuzu_buffer_pool_mb`` and ``kg_kuzu_max_db_size_gb`` from
    :class:`CoreSettings` and passes them in bytes. Replaces Kùzu's own
    defaults (``buffer_pool_size=0`` → ~80% system RAM, ``max_db_size=1<<43``
    → 8 TB VA) which caused 128 GB RSS with 3 instances in field reports.

    Keeping this factory as the unique entry point lets ops re-tune memory
    from a single place and enables the runtime Settings menu (0.1.4).

    Raises a clear ``RuntimeError`` when the underlying Kùzu storage is
    corrupted due to a version incompatibility (SIGBUS / BusError) instead
    of letting the signal crash the process.
    """
    import ladybug as kuzu  # type: ignore
    from okto_pulse.core.infra.config import get_settings

    logger.debug("[KG] _open_kuzu_db path=%s", path)
    s = get_settings()
    bp = s.kg_kuzu_buffer_pool_mb * 1024 * 1024
    mds = s.kg_kuzu_max_db_size_gb * 1024 * 1024 * 1024
    logger.debug("[KG] _open_kuzu_db buffer_pool=%dMB max_db=%dGB", s.kg_kuzu_buffer_pool_mb, s.kg_kuzu_max_db_size_gb)

    # Bug d0f6bab2: lock contention happens because every BoardConnection
    # used to spawn a NEW kuzu.Database — but Kùzu locks the .kuzu dir at
    # the OS level for as long as ANY Database handle exists. When two
    # workers (consolidation + handler) open the same board, the second
    # blocks. Retry+gc.collect() does not help when the contention is
    # cross-thread.  Real fix: cache Database per path (singleton) so
    # multiple Connections share one Database (Kùzu supports that). The
    # caller-facing API (BoardConnection / open_board_connection) uses
    # `_open_kuzu_db_cached` which delegates here only on cache miss.
    #
    # Retry below covers the residual case where ANOTHER process holds
    # the lock (e.g. CLI run while server is up). 5× exponential backoff:
    # 0.2 / 0.4 / 0.8 / 1.6 / 3.2 = 6.2s cumulative.
    last_exc: BaseException | None = None
    for attempt in range(1, 6):
        try:
            db = kuzu.Database(
                str(path),
                buffer_pool_size=bp,
                max_db_size=mds,
            )
            logger.debug("[KG] kuzu.Database() created successfully for path=%s", path)
            return db
        except Exception as e:
            last_exc = e
            msg = str(e)
            is_lock_contention = "Could not set lock" in msg or "lock contention" in msg.lower()
            if is_lock_contention and attempt < 5:
                sleep_s = 0.2 * (2 ** (attempt - 1))
                logger.warning(
                    "kg.db_open.lock_retry path=%s attempt=%d/5 sleep=%.2fs err=%s",
                    path, attempt, sleep_s, e,
                    extra={
                        "event": "kg.db_open.lock_retry",
                        "path": str(path),
                        "attempt": attempt,
                        "sleep_s": sleep_s,
                    },
                )
                gc.collect()  # Liberar handles pendentes (essencial no Windows)
                time.sleep(sleep_s)
                continue
            break

    e = last_exc  # type: ignore[assignment]
    logger.error(
        "[KG] Failed to open Kùzu database at %s: %s: %s",
        path, type(e).__name__, e,
    )
    raise RuntimeError(
        f"Failed to open Kùzu database at {path}: "
        f"{type(e).__name__}: {e}. "
        "Possible causes: "
        "(1) lock contention from concurrent writer (wait and retry); "
        "(2) schema migration needed — run "
        "`python -m okto_pulse.tools.kg_migrate_schema --board <board_id>` "
        "or call MCP tool `okto_pulse_kg_migrate_schema`; "
        "(3) corrupted db file. "
        "Do NOT delete graph.kuzu (destroys all KG data)."
    ) from e


def verify_kuzu_db_health(board_id: str) -> dict[str, Any]:
    """Check if the Kùzu database for a board is healthy.

    Returns a dict with ``ok`` (bool), ``node_count`` (int), and ``error``
    (str or None). Safe to call during server startup or as a health-check
    endpoint — opens a short-lived connection, counts all nodes, and closes
    cleanly.
    """
    path = board_kuzu_path(board_id)
    if not path.exists():
        return {"ok": True, "node_count": 0, "error": None,
                "note": "graph.kuzu does not exist yet — will be created on first access"}
    try:
        with open_board_connection(board_id) as (db, conn):
            res = conn.execute("MATCH (n) RETURN count(n) AS cnt")
            row = res.get_next()
            cnt = int(row[0])
            res.close()
        return {"ok": True, "node_count": cnt, "error": None}
    except Exception as e:
        return {"ok": False, "node_count": 0, "error": f"{type(e).__name__}: {e}"}


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


def _is_duplicate_column_error(exc: BaseException) -> bool:
    """Kùzu raises Binder exceptions on duplicate ADD. Recognize the benign
    idempotent case so we can distinguish it from genuine errors (lock
    contention, permission, etc.)."""
    msg = str(exc).lower()
    return (
        "already exists" in msg
        or "duplicate" in msg
        or "already has property" in msg
    )


def _is_retryable_kuzu_error(exc: BaseException) -> bool:
    """File-lock and transient IO errors on the embedded .kuzu file.

    Windows file locking is the dominant offender: when the board graph
    is open for reads (e.g. fallback search) an ALTER concurrent with the
    reader fails with ``IO exception: Could not set lock on file``.
    """
    msg = str(exc).lower()
    return (
        "could not set lock" in msg
        or "io exception" in msg
        or "timeout" in msg
    )


def _alter_add_column_with_retry(
    conn, node_type: str, col_name: str, col_type: str,
    *, max_attempts: int = 5, base_sleep: float = 0.2,
) -> str:
    """ALTER TABLE ADD with retry on lock contention.

    Returns one of: ``"added"``, ``"exists"``, ``"failed"``. ``"exists"`` is
    the idempotent path (column already present). ``"failed"`` means all
    retries exhausted on a retryable error OR a non-retryable error — either
    case logged at WARN so silent swallowing doesn't keep hiding schema
    drift like the 2026-04-19 priority_boost incident.
    """
    ddl = f"ALTER TABLE {node_type} ADD {col_name} {col_type}"
    last_err: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn.execute(ddl)
            return "added"
        except Exception as exc:
            last_err = exc
            if _is_duplicate_column_error(exc):
                return "exists"
            if _is_retryable_kuzu_error(exc) and attempt < max_attempts:
                sleep_s = base_sleep * (2 ** (attempt - 1))
                logger.info(
                    "kg.schema.alter_retry node=%s col=%s attempt=%d/%d sleep=%.2fs err=%s",
                    node_type, col_name, attempt, max_attempts, sleep_s, exc,
                )
                time.sleep(sleep_s)
                continue
            break
    logger.warning(
        "kg.schema.alter_failed node=%s col=%s attempts=%d err=%s",
        node_type, col_name, max_attempts, last_err,
    )
    return "failed"


def _ensure_relevance_columns(conn, node_type: str) -> list[str]:
    """ALTER TABLE ADD for every v0.3.0 column missing on ``node_type``.

    Idempotent — retries on lock contention (see
    :func:`_alter_add_column_with_retry`). Returns the list of columns
    actually added this call.
    """
    added: list[str] = []
    for col_name, col_type in RELEVANCE_COLUMNS:
        if _alter_add_column_with_retry(conn, node_type, col_name, col_type) == "added":
            added.append(col_name)
    return added


def _ensure_priority_boost_columns(conn, node_type: str) -> list[str]:
    """ALTER TABLE ADD for the v0.3.1 priority_boost column on ``node_type``.

    Idempotent with retry on lock contention. Returns the list of columns
    actually added this call (typically empty on second run).
    """
    added: list[str] = []
    for col_name, col_type in PRIORITY_BOOST_COLUMNS:
        if _alter_add_column_with_retry(conn, node_type, col_name, col_type) == "added":
            added.append(col_name)
    return added


def _ensure_human_curated_columns(conn, node_type: str) -> list[str]:
    """ALTER TABLE ADD for the v0.3.2 human_curated column on ``node_type``.

    Idempotent with retry on lock contention. Default treatment for legacy
    nodes (NULL value): the UPDATE preservation path treats NULL as FALSE,
    so no backfill is required for retrocompat. Curators set TRUE
    explicitly via back-office tooling after manual edits.
    """
    added: list[str] = []
    for col_name, col_type in HUMAN_CURATED_COLUMNS:
        if _alter_add_column_with_retry(conn, node_type, col_name, col_type) == "added":
            added.append(col_name)
    return added


def _ensure_last_recomputed_at_columns(conn, node_type: str) -> list[str]:
    """ALTER TABLE ADD for the v0.3.3 last_recomputed_at column on ``node_type``.

    Idempotent with retry on lock contention. Legacy rows get NULL — the
    daily decay tick treats NULL as "never recomputed" and prioritises those
    nodes first when sizing its workload. No backfill required.
    """
    added: list[str] = []
    for col_name, col_type in LAST_RECOMPUTED_COLUMNS:
        if _alter_add_column_with_retry(conn, node_type, col_name, col_type) == "added":
            added.append(col_name)
    return added


def _backfill_relevance_defaults(conn, node_type: str) -> None:
    """Populate the v0.3.0 columns for rows that existed before the migration.

    Sets relevance_score=0.5 and query_hits=0 only where the value is NULL,
    keeping the migration re-runnable. last_queried_at stays NULL — it will be
    populated organically by the R2 hit-counter path.
    """
    try:
        conn.execute(
            f"MATCH (n:{node_type}) "
            f"WHERE n.relevance_score IS NULL "
            f"SET n.relevance_score = 0.5"
        )
    except Exception as exc:
        logger.warning(
            "migrate_relevance.backfill_failed node=%s col=relevance_score err=%s",
            node_type, exc,
        )
    try:
        conn.execute(
            f"MATCH (n:{node_type}) "
            f"WHERE n.query_hits IS NULL "
            f"SET n.query_hits = 0"
        )
    except Exception as exc:
        logger.warning(
            "migrate_relevance.backfill_failed node=%s col=query_hits err=%s",
            node_type, exc,
        )


def _node_has_legacy_columns(conn, node_type: str) -> bool:
    """Returns True iff ``node_type`` still has validation_status /
    corroboration_count columns from v0.2.0. Uses the table info catalog.

    A best-effort probe — any error is treated as "no legacy columns" so we
    don't try to re-drop on a fresh v0.3.0 board.
    """
    res = None
    try:
        res = conn.execute(f"CALL TABLE_INFO('{node_type}') RETURN *")
        cols: set[str] = set()
        while res.has_next():
            row = res.get_next()
            # TABLE_INFO returns columns including "name" somewhere in the row;
            # normalise by iterating.
            for item in row:
                if isinstance(item, str):
                    cols.add(item)
        return any(c in cols for c in LEGACY_NODE_COLUMNS)
    except Exception:
        return False
    finally:
        if res is not None:
            try:
                res.close()
            except Exception:
                pass


def _node_has_relevance_columns(conn, node_type: str) -> bool:
    """Returns True iff ``node_type`` already has the v0.3.0 columns."""
    res = None
    try:
        res = conn.execute(f"CALL TABLE_INFO('{node_type}') RETURN *")
        cols: set[str] = set()
        while res.has_next():
            row = res.get_next()
            for item in row:
                if isinstance(item, str):
                    cols.add(item)
        return all(c in cols for c in (name for name, _ in RELEVANCE_COLUMNS))
    except Exception:
        return False
    finally:
        if res is not None:
            try:
                res.close()
            except Exception:
                pass


def _migrate_node_table_v030(conn, node_type: str) -> int:
    """Drop + recreate a node table with the v0.3.0 schema, preserving rows.

    Kùzu v0.6 has no ALTER TABLE DROP COLUMN, so when validation_status /
    corroboration_count must go we have to:

      1. dump every row via ``MATCH (n:Type) RETURN n.*``
      2. ``DROP NODE TABLE Type``
      3. ``CREATE NODE TABLE Type (...)`` with the new schema
      4. re-insert the dumped rows, mapping legacy cols onto the new defaults

    Returns the number of rows migrated (best-effort — 0 when the driver
    doesn't expose a count). The caller is expected to recreate any vector
    index on the table afterwards (``CREATE_VECTOR_INDEX`` is idempotent and
    lives in ``bootstrap_board_graph``).
    """
    dumped: list[dict[str, Any]] = []
    res = None
    try:
        res = conn.execute(
            f"MATCH (n:{node_type}) RETURN n.id AS id, n.title AS title, "
            f"n.content AS content, n.context AS context, "
            f"n.justification AS justification, "
            f"n.source_artifact_ref AS source_artifact_ref, "
            f"n.source_session_id AS source_session_id, "
            f"n.created_at AS created_at, n.created_by_agent AS created_by_agent, "
            f"n.source_confidence AS source_confidence, "
            f"n.superseded_by AS superseded_by, "
            f"n.superseded_at AS superseded_at, "
            f"n.revocation_reason AS revocation_reason, "
            f"n.embedding AS embedding"
        )
        while res.has_next():
            row = res.get_next()
            # Row is positional — map to column names in the SELECT order.
            dumped.append({
                "id": row[0],
                "title": row[1],
                "content": row[2],
                "context": row[3],
                "justification": row[4],
                "source_artifact_ref": row[5],
                "source_session_id": row[6],
                "created_at": row[7],
                "created_by_agent": row[8],
                "source_confidence": row[9],
                "superseded_by": row[10],
                "superseded_at": row[11],
                "revocation_reason": row[12],
                "embedding": row[13],
            })
    except Exception as exc:
        logger.warning(
            "migrate_v030.dump_failed node=%s err=%s — skipping table",
            node_type, exc,
        )
        return 0
    finally:
        if res is not None:
            try:
                res.close()
            except Exception:
                pass

    try:
        conn.execute(f"DROP TABLE {node_type}")
    except Exception as exc:
        logger.warning(
            "migrate_v030.drop_failed node=%s err=%s — table may be in use",
            node_type, exc,
        )
        return 0

    try:
        conn.execute(_build_node_ddl(node_type))
    except Exception as exc:
        logger.error(
            "migrate_v030.create_failed node=%s err=%s — data loss risk",
            node_type, exc,
        )
        raise

    restored = 0
    for row in dumped:
        try:
            conn.execute(
                f"CREATE (n:{node_type} {{"
                f"id: $id, title: $title, content: $content, context: $context, "
                f"justification: $justification, "
                f"source_artifact_ref: $source_artifact_ref, "
                f"source_session_id: $source_session_id, "
                f"created_at: $created_at, created_by_agent: $created_by_agent, "
                f"source_confidence: $source_confidence, "
                f"relevance_score: 0.5, query_hits: 0, last_queried_at: NULL, "
                f"priority_boost: 0.0, "
                f"superseded_by: $superseded_by, superseded_at: $superseded_at, "
                f"revocation_reason: $revocation_reason, embedding: $embedding"
                f"}})",
                row,
            )
            restored += 1
        except Exception as exc:
            logger.warning(
                "migrate_v030.restore_failed node=%s id=%s err=%s",
                node_type, row.get("id"), exc,
            )

    logger.info(
        "migrate_v030.table_done node=%s dumped=%d restored=%d",
        node_type, len(dumped), restored,
        extra={"event": "migrate_v030.table_done", "node_type": node_type,
               "dumped": len(dumped), "restored": restored},
    )
    return restored


def migrate_board_to_v030(board_id: str) -> dict[str, Any]:
    """Apply the v0.2.0 → v0.3.0 migration to a board.

    Idempotent, non-destructive. For every node table:

      * ``ALTER TABLE ADD`` the three v0.3.0 columns when missing.
      * Backfill ``relevance_score = 0.5`` / ``query_hits = 0`` where NULL.

    Kùzu v0.6 does not allow ``DROP NODE TABLE`` while rel tables or
    vector indexes reference it, so we leave the legacy
    ``validation_status`` / ``corroboration_count`` columns in place as
    orphans. The Python code no longer reads them — they are harmless
    dead data until a future hard reset.

    Vector indexes remain intact; the migration never drops them.

    Returns a summary ``{node_type: {"strategy": "alter", "added": [...]}}``
    for audit logs.
    """
    summary: dict[str, Any] = {}
    path = board_kuzu_path(board_id)
    if not path.exists():
        return summary

    close_all_connections(board_id)

    # Use a raw kuzu.Connection here — open_board_connection() would
    # re-enter _board_needs_v030_migration and recurse infinitely, and
    # the migration must run BEFORE the BoardConnection bootstrap path
    # ever owns the handle.
    import ladybug as kuzu  # type: ignore
    db = _open_kuzu_db_path_cached(path)
    conn = kuzu.Connection(db)
    try:
        for node_type in NODE_TYPES:
            added = _ensure_relevance_columns(conn, node_type)
            _backfill_relevance_defaults(conn, node_type)
            had_legacy = _node_has_legacy_columns(conn, node_type)
            summary[node_type] = {
                "strategy": "alter",
                "added": added,
                "legacy_columns_left": had_legacy,
            }

        try:
            conn.execute(
                "MATCH (m:BoardMeta {board_id: $bid}) "
                "SET m.schema_version = $v",
                {"bid": board_id, "v": SCHEMA_VERSION},
            )
        except Exception as exc:
            logger.warning(
                "migrate_v030.meta_update_failed board=%s err=%s",
                board_id, exc,
            )
    finally:
        try:
            conn.close()
        except Exception:
            pass
        # Bug d0f6bab2: db is now process-cached (_board_db_cache); do NOT
        # close it here or concurrent BoardConnections lose the lock.
        # Cache is dropped explicitly via close_board_db_cache().
        gc.collect()

    _MIGRATED_BOARDS.add(board_id)

    logger.info(
        "migrate_v030.done board=%s summary=%s",
        board_id, summary,
        extra={"event": "migrate_v030.done", "board_id": board_id,
               "summary": summary},
    )
    return summary


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
        import ladybug as kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        res = None
        try:
            res = conn.execute("CALL show_tables() WHERE type='REL' RETURN name")
            existing = set()
            while res.has_next():
                existing.add(res.get_next()[0])
        finally:
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
        expected = {r[0] for r in REL_TYPES} | {m[0] for m in MULTI_REL_TYPES}
        if expected.issubset(existing):
            _MIGRATED_BOARDS.add(board_id)
            return False
        return True
    except Exception:
        # Probe failed — assume migration is needed; the apply itself is
        # idempotent so a false positive only costs one extra DDL pass.
        return True


def _board_needs_priority_boost_migration(board_id: str) -> bool:
    """Returns True iff the board is missing the v0.3.1 ``priority_boost``
    column on any node type.

    Fixes the 2026-04-19 incident where boards bootstrapped before the
    ``7c032ee`` commit had rel tables + v0.3.0 columns (so the other two
    probes short-circuited) but lacked ``priority_boost``, silently
    breaking every ``commit_consolidation`` with a Binder exception.

    Column-based probe over the first node type — authoritative regardless
    of BoardMeta.schema_version. Returns False on probe failure so a stuck
    probe never loops the migration.
    """
    try:
        import ladybug as kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        res = None
        try:
            # TABLE_INFO on the first node type is representative: the
            # migration adds priority_boost to every node type in a loop,
            # so if any one is missing, all are missing (legacy boards
            # were bootstrapped in one pass, not incrementally).
            probe_node = NODE_TYPES[0]
            res = conn.execute(f"CALL TABLE_INFO('{probe_node}') RETURN *")
            existing_cols: set[str] = set()
            while res.has_next():
                row = res.get_next()
                # TABLE_INFO row: [index, name, type, default, pk]
                existing_cols.add(str(row[1]))
        finally:
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
        expected = {c for c, _ in PRIORITY_BOOST_COLUMNS}
        return not expected.issubset(existing_cols)
    except Exception:
        return False


def _board_needs_v030_migration(board_id: str) -> bool:
    """Returns True iff the board is missing the v0.3.0 node columns.

    The probe is column-based — it does NOT trust BoardMeta.schema_version
    alone because an earlier destructive-migration attempt may have bumped
    the recorded version without actually adding the ALTER columns (if a
    DROP TABLE failed against a referenced rel/index). Inspecting
    ``TABLE_INFO`` on the first node type is the authoritative answer.

    Returns False on any probe error so a broken probe never loops a
    destructive re-migration.
    """
    if board_id in _MIGRATED_BOARDS:
        return False
    try:
        import ladybug as kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        if not path.exists():
            return False
        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        try:
            for node_type in NODE_TYPES:
                if not _node_has_relevance_columns(conn, node_type):
                    return True
                break  # one probe is enough — all node types share _COMMON_NODE_ATTRS
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
    except Exception:
        return False


def _board_needs_human_curated_migration(board_id: str) -> bool:
    """Returns True iff the board is missing the v0.3.2 ``human_curated``
    column on any node type.

    Spec 818748f2 — FR2. Same column-based pattern as
    `_board_needs_priority_boost_migration` (L918). The migration adds
    ``human_curated`` to every node type in a single loop, so probing the
    first node is representative — if one is missing, all are missing.

    Returns False on probe failure (BR6: silent in failure to NOT loop
    a stuck migration).
    """
    try:
        import ladybug as kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        res = None
        try:
            probe_node = NODE_TYPES[0]
            res = conn.execute(f"CALL TABLE_INFO('{probe_node}') RETURN *")
            existing_cols: set[str] = set()
            while res.has_next():
                row = res.get_next()
                existing_cols.add(str(row[1]))
        finally:
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
        expected = {c for c, _ in HUMAN_CURATED_COLUMNS}
        return not expected.issubset(existing_cols)
    except Exception:
        return False


def _board_needs_last_recomputed_migration(board_id: str) -> bool:
    """Returns True iff the board is missing the v0.3.3
    ``last_recomputed_at`` column on any node type.

    Spec 818748f2 — FR2. Same column-based pattern as the priority_boost
    and human_curated probes. Returns False on probe failure (BR6).
    """
    try:
        import ladybug as kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        res = None
        try:
            probe_node = NODE_TYPES[0]
            res = conn.execute(f"CALL TABLE_INFO('{probe_node}') RETURN *")
            existing_cols: set[str] = set()
            while res.has_next():
                row = res.get_next()
                existing_cols.add(str(row[1]))
        finally:
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
        expected = {c for c, _ in LAST_RECOMPUTED_COLUMNS}
        return not expected.issubset(existing_cols)
    except Exception:
        return False


def _board_needs_post_v030_migration(board_id: str) -> bool:
    """Compose probe — True iff any v0.3.1+ column is missing on the board.

    Spec 818748f2 — FR3. Aggregates the three column probes (priority_boost,
    human_curated, last_recomputed_at) via short-circuit OR. Cache hit via
    `_MIGRATED_BOARDS` makes this a no-op after the first migration succeeds
    (BR1: idempotent re-runs cost only the cache lookup).

    Probes ordered chronologically (v0.3.1 → v0.3.2 → v0.3.3). Boards that
    are most behind short-circuit at the earliest probe.
    """
    if board_id in _MIGRATED_BOARDS:
        return False
    return (
        _board_needs_priority_boost_migration(board_id)
        or _board_needs_human_curated_migration(board_id)
        or _board_needs_last_recomputed_migration(board_id)
    )


def _migrate_board_schema(board_id: str) -> None:
    """One-shot schema apply for a pre-existing board. Wraps the DDL pass
    in its own short-lived connection so the caller's connection lifecycle
    isn't tangled with the migration's, then caches the board as migrated."""
    try:
        import ladybug as kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        try:
            apply_schema_to_connection(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
        _MIGRATED_BOARDS.add(board_id)
    except Exception as exc:
        logger.warning(
            "board_migrate.apply_failed board=%s err=%s",
            board_id, exc,
        )


def migrate_schema_for_board(board_id: str) -> dict[str, Any]:
    """Force-apply schema migrations for a single board (idempotent).

    Spec 818748f2 (FR5). Public surface for the CLI/MCP/REST tripleta —
    re-runs ALTER TABLE ADD for every v0.3.x column on every node type and
    returns a structured summary so callers can display columns added per
    node type.

    Differs from `_migrate_board_schema`:
    - Discards `_MIGRATED_BOARDS` cache for this board so the migration
      re-runs even if a previous attempt cached the board (BR1: idempotent
      means re-runnable, not skip-after-first-success).
    - Captures columns_added per node type via the existing return values
      from `_ensure_*_columns` (which already track ALTER ADD success).
    - Surfaces errors as a list (non-fatal) instead of swallowing.
    - Returns timing for observability.

    Args:
        board_id: Board ID to migrate.

    Returns:
        ``{"board_id": str, "migrated": bool, "columns_added":
        {node_type: [col_name]}, "errors": [str], "duration_ms": int}``
    """
    start = time.time()
    columns_added: dict[str, list[str]] = {}
    errors: list[str] = []
    migrated = False

    # BR1: idempotent re-run requires invalidating the cache.
    _MIGRATED_BOARDS.discard(board_id)

    try:
        path = board_kuzu_path(board_id)
        if not path.exists():
            errors.append(
                f"board_not_found: graph.kuzu missing at {path}"
            )
            return {
                "board_id": board_id,
                "migrated": False,
                "columns_added": columns_added,
                "errors": errors,
                "duration_ms": int((time.time() - start) * 1000),
            }

        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        try:
            load_vector_extension(conn)
            conn.execute(_board_meta_ddl())
            for node_type in NODE_TYPES:
                added_for_type: list[str] = []
                try:
                    conn.execute(_build_node_ddl(node_type))
                    added_for_type.extend(
                        _ensure_priority_boost_columns(conn, node_type)
                    )
                    added_for_type.extend(
                        _ensure_human_curated_columns(conn, node_type)
                    )
                    added_for_type.extend(
                        _ensure_last_recomputed_at_columns(conn, node_type)
                    )
                except Exception as nt_exc:
                    errors.append(
                        f"node_type_failed: {node_type}: {nt_exc}"
                    )
                if added_for_type:
                    columns_added[node_type] = added_for_type
            for rel_name, from_type, to_type in REL_TYPES:
                try:
                    conn.execute(_build_rel_ddl(rel_name, from_type, to_type))
                    _ensure_edge_metadata_columns(conn, rel_name)
                    _backfill_legacy_edge_metadata(conn, rel_name)
                except Exception as rel_exc:
                    errors.append(
                        f"rel_failed: {rel_name}: {rel_exc}"
                    )
            for rel_name, pairs in MULTI_REL_TYPES:
                try:
                    conn.execute(_build_multi_rel_ddl(rel_name, pairs))
                    _ensure_edge_metadata_columns(conn, rel_name)
                    _backfill_legacy_edge_metadata(conn, rel_name)
                except Exception as mrel_exc:
                    errors.append(
                        f"multi_rel_failed: {rel_name}: {mrel_exc}"
                    )
        finally:
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
        # BR3: only cache as migrated if migration actually completed.
        # We treat "no errors" as success even if columns_added is empty
        # (idempotent no-op on already-migrated boards).
        if not errors:
            _MIGRATED_BOARDS.add(board_id)
            migrated = True
        else:
            # Partial migration — some node/rel types may have applied
            # but at least one failed. Don't cache so the next open retries.
            migrated = False
    except Exception as exc:
        errors.append(f"migration_failed: {exc}")
        migrated = False
        logger.warning(
            "kg.migrate_schema.failed board=%s err=%s",
            board_id, exc,
            extra={
                "event": "kg.migrate_schema.failed",
                "board_id": board_id,
                "error": str(exc),
            },
        )

    duration_ms = int((time.time() - start) * 1000)
    logger.info(
        "kg.migrate_schema.done board=%s migrated=%s columns_added=%s "
        "errors=%d duration_ms=%d",
        board_id, migrated, columns_added, len(errors), duration_ms,
        extra={
            "event": "kg.migrate_schema.done",
            "board_id": board_id,
            "migrated": migrated,
            "columns_added_count": sum(len(v) for v in columns_added.values()),
            "errors_count": len(errors),
            "duration_ms": duration_ms,
        },
    )
    return {
        "board_id": board_id,
        "migrated": migrated,
        "columns_added": columns_added,
        "errors": errors,
        "duration_ms": duration_ms,
    }


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
        # v0.3.1: ensure priority_boost column exists on legacy boards. The
        # CREATE TABLE IF NOT EXISTS above is a no-op on pre-existing tables
        # so we still need to run the ALTER ADD path to backfill the column.
        _ensure_priority_boost_columns(conn, node_type)
        # v0.3.2 (spec 4007e4a3): human_curated marks human-edited nodes that
        # the agent UPDATE path must skip without explicit override.
        _ensure_human_curated_columns(conn, node_type)
        # v0.3.3 (spec 28583299 — Ideação #4): last_recomputed_at is the
        # ISO timestamp of the last relevance_score persist. Read by the
        # daily decay tick and kg_health for observability.
        _ensure_last_recomputed_at_columns(conn, node_type)
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
        import ladybug as kuzu  # type: ignore
    except ImportError as exc:  # pragma: no cover — deps required for runtime
        raise RuntimeError(
            "kuzu is required for the knowledge graph layer — "
            "install with `pip install kuzu`"
        ) from exc

    path = board_kuzu_path(board_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    db = _open_kuzu_db_path_cached(path)
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
        try:
            conn.close()
        except Exception:
            pass
        # Bug d0f6bab2: db is now process-cached (_board_db_cache); do NOT
        # close it here or concurrent BoardConnections lose the lock.
        # Cache is dropped explicitly via close_board_db_cache().

    return BoardGraphHandle(board_id=board_id, path=path, schema_version=SCHEMA_VERSION)


# Process-local cache of boards whose Kùzu graph has been bootstrapped in
# this process. Parallel to _MIGRATED_BOARDS but for the cold-path
# bootstrap step, not the ALTER-based migrations. Populated by
# ensure_board_graph_bootstrapped() which is called from BoardConnection.
_BOOTSTRAPPED_BOARDS: set[str] = set()

# Per-board lock so concurrent openers serialize the bootstrap. Kùzu's own
# file lock would catch cross-process races, but within one process asyncio
# tasks could both see the empty cache and both try to bootstrap.
_BOOTSTRAP_LOCKS: dict[str, threading.Lock] = {}
_BOOTSTRAP_LOCKS_GUARD = threading.Lock()


def _get_bootstrap_lock(board_id: str) -> threading.Lock:
    with _BOOTSTRAP_LOCKS_GUARD:
        lock = _BOOTSTRAP_LOCKS.get(board_id)
        if lock is None:
            lock = threading.Lock()
            _BOOTSTRAP_LOCKS[board_id] = lock
        return lock


def _graph_needs_bootstrap(board_id: str) -> bool:
    """Cheap probe: does the board's .kuzu path exist AND contain the
    BoardMeta node table?

    Returns True when the graph is missing entirely OR when it exists but
    lacks the BoardMeta table (signaling a partial bootstrap). False when
    the graph is present AND BoardMeta exists.
    """
    if board_id in _BOOTSTRAPPED_BOARDS:
        return False
    path = board_kuzu_path(board_id)
    if not path.exists():
        return True
    try:
        import ladybug as kuzu  # type: ignore
        db = _open_kuzu_db_path_cached(path)
        conn = kuzu.Connection(db)
        try:
            res = conn.execute(
                "CALL SHOW_TABLES() WHERE name = 'BoardMeta' RETURN name"
            )
            has_meta = res.has_next()
            res.close()
        finally:
            try:
                conn.close()
            except Exception:
                pass
            # Bug d0f6bab2: db is now process-cached; do NOT close here.
        if has_meta:
            _BOOTSTRAPPED_BOARDS.add(board_id)
            return False
        return True
    except Exception as exc:
        logger.debug(
            "kg.schema.bootstrap_probe_failed board=%s err=%s — will bootstrap",
            board_id, exc,
        )
        return True


def ensure_board_graph_bootstrapped(board_id: str) -> None:
    """Idempotent, thread-safe guarantee that the board's Kùzu graph exists
    with the current schema. Safe to call from any entry point — CLI, API,
    worker, MCP tool, search, health check.

    Called automatically by BoardConnection.__init__ so direct callers of
    open_board_connection and all primitives that open connections get
    the guarantee for free.

    Spec 818748f2 — FR1: when BoardMeta exists but post-v0.3.0 columns are
    missing (legacy boards bootstrapped pre-v0.3.2), `_migrate_board_schema`
    is dispatched in the same lock window so the next consolidation does not
    hit a binder exception. Cache add to `_BOOTSTRAPPED_BOARDS` happens AFTER
    the migration completes (BR3 — never cache a broken state).
    """
    if board_id in _BOOTSTRAPPED_BOARDS:
        return
    lock = _get_bootstrap_lock(board_id)
    with lock:
        if board_id in _BOOTSTRAPPED_BOARDS:
            return
        if _graph_needs_bootstrap(board_id):
            logger.info(
                "kg.schema.autobootstrap board=%s path=%s",
                board_id, board_kuzu_path(board_id),
                extra={"event": "kg.schema.autobootstrap", "board_id": board_id},
            )
            bootstrap_board_graph(board_id)
        elif _board_needs_post_v030_migration(board_id):
            logger.info(
                "kg.schema.auto_migrate_post_v030 board=%s path=%s",
                board_id, board_kuzu_path(board_id),
                extra={
                    "event": "kg.schema.auto_migrate_post_v030",
                    "board_id": board_id,
                },
            )
            _migrate_board_schema(board_id)
        _BOOTSTRAPPED_BOARDS.add(board_id)


def reset_bootstrap_cache_for_tests() -> None:
    """Test helper — clear the process-local bootstrap cache so the next
    open triggers a fresh bootstrap probe. Call from pytest fixtures that
    delete board directories mid-test."""
    _BOOTSTRAPPED_BOARDS.clear()
    with _BOOTSTRAP_LOCKS_GUARD:
        _BOOTSTRAP_LOCKS.clear()


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
        # Bug d0f6bab2: drop the cached Database for this board so a
        # follow-up rmtree (or migration) can grab the OS lock.
        close_board_db_cache(board_id=board_id)
        return

    if close_all_board_connections is not None:
        try:
            close_all_board_connections()
        except Exception as exc:
            logger.warning(
                "close_all.pool_failed err=%s", exc,
                extra={"event": "close_all.pool_failed"},
            )

    # Bug d0f6bab2: also drop the per-board Database cache so the OS file
    # lock is released. Without this, even after legacy pool eviction the
    # BoardConnection cache keeps the .kuzu dir locked.
    close_board_db_cache(board_id=None)

    # global is released only when closing everything — per-board callers
    # (e.g. single-board DELETE) must not nuke the shared discovery handle.
    from okto_pulse.core.kg.global_discovery.schema import close_global_connection

    close_global_connection()


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
