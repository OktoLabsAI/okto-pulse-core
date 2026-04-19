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

SCHEMA_VERSION = "0.3.0"

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
    priority_boost DOUBLE,
    superseded_by STRING,
    superseded_at TIMESTAMP,
    revocation_reason STRING,
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

# Columns removed in v0.3.0. Kùzu v0.6 has no ALTER TABLE DROP COLUMN, so the
# migration strategy is dump→drop→create→bulk-insert when these are detected.
LEGACY_NODE_COLUMNS: tuple[str, ...] = ("validation_status", "corroboration_count")


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


def _ensure_relevance_columns(conn, node_type: str) -> list[str]:
    """ALTER TABLE ADD for every v0.3.0 column missing on ``node_type``.

    Returns the list of columns actually added. Kùzu raises on duplicate ADD
    so we catch-and-continue — the operation is idempotent. Used when the
    node table already exists but was bootstrapped under v0.2.0.
    """
    added: list[str] = []
    for col_name, col_type in RELEVANCE_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE {node_type} ADD {col_name} {col_type}")
            added.append(col_name)
        except Exception:
            pass
    return added


def _ensure_priority_boost_columns(conn, node_type: str) -> list[str]:
    """ALTER TABLE ADD for the v0.3.1 priority_boost column on ``node_type``.

    Idempotent — Kùzu raises on duplicate ADD so we catch-and-continue.
    Returns the list of columns actually added (typically empty on second
    run). Invoked from apply_schema_to_connection so every board gets the
    column regardless of when it was first bootstrapped.
    """
    added: list[str] = []
    for col_name, col_type in PRIORITY_BOOST_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE {node_type} ADD {col_name} {col_type}")
            added.append(col_name)
        except Exception:
            pass
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


def _node_has_relevance_columns(conn, node_type: str) -> bool:
    """Returns True iff ``node_type`` already has the v0.3.0 columns."""
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
    import kuzu  # type: ignore
    db = kuzu.Database(str(path))
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
        del conn
        del db
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
        import kuzu  # type: ignore
        path = board_kuzu_path(board_id)
        if not path.exists():
            return False
        db = kuzu.Database(str(path))
        conn = kuzu.Connection(db)
        try:
            for node_type in NODE_TYPES:
                if not _node_has_relevance_columns(conn, node_type):
                    return True
                break  # one probe is enough — all node types share _COMMON_NODE_ATTRS
            return False
        finally:
            del conn, db
    except Exception:
        return False


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
        # v0.3.1: ensure priority_boost column exists on legacy boards. The
        # CREATE TABLE IF NOT EXISTS above is a no-op on pre-existing tables
        # so we still need to run the ALTER ADD path to backfill the column.
        _ensure_priority_boost_columns(conn, node_type)
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
        else:
            # v0.3.0 pivot: if the board still carries validation_status /
            # corroboration_count, run the destructive column migration BEFORE
            # the rel-table apply so apply_schema_to_connection operates on
            # the new DDL. Idempotent: the probe short-circuits once the
            # BoardMeta version is bumped or the cache is populated.
            if _board_needs_v030_migration(board_id):
                try:
                    migrate_board_to_v030(board_id)
                except Exception as exc:
                    logger.warning(
                        "board_v030_migrate.failed board=%s err=%s",
                        board_id, exc,
                    )
            if _board_needs_migration(board_id):
                # Pre-existing board missing a rel table (e.g. `belongs_to`
                # added post-bootstrap). Run schema apply ONCE to backfill,
                # then mark the board as migrated so subsequent opens skip
                # the (write-heavy) DDL pass.
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
