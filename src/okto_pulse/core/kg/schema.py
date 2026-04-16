"""Kùzu per-board graph schema — 11 node tables, 10 rel tables, 5 vector indexes.

Idempotent bootstrap: `bootstrap_board_graph(board_id)` creates or opens the
per-board `.kuzu` directory under `kg_base_dir/boards/{board_id}/graph.kuzu`,
applies DDL, creates HNSW vector indexes for searchable node types, and
records the schema version on a Board meta node.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SCHEMA_VERSION = "0.1.0"

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
    return (
        f"CREATE REL TABLE IF NOT EXISTS {rel_name} "
        f"(FROM {from_type} TO {to_type}, "
        f"confidence DOUBLE, "
        f"created_by_session_id STRING, "
        f"created_at TIMESTAMP)"
    )


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
        load_vector_extension(conn)

        conn.execute(_board_meta_ddl())
        for node_type in NODE_TYPES:
            conn.execute(_build_node_ddl(node_type))
        for rel_name, from_type, to_type in REL_TYPES:
            conn.execute(_build_rel_ddl(rel_name, from_type, to_type))

        # Vector indexes: one HNSW index per searchable node type. Kùzu 0.11
        # CREATE_VECTOR_INDEX takes (table, idx_name, col_name) positional.
        # Wrap each call individually so a pre-existing index doesn't abort
        # the whole bootstrap.
        for node_type in VECTOR_INDEX_TYPES:
            idx = vector_index_name(node_type)
            try:
                conn.execute(
                    f"CALL CREATE_VECTOR_INDEX("
                    f"'{node_type}', '{idx}', 'embedding')"
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

    return BoardGraphHandle(
        board_id=board_id,
        path=path,
        schema_version=SCHEMA_VERSION,
    )


def open_board_connection(board_id: str):
    """Open a fresh Kùzu connection for a board, ensuring vector extension loaded.

    Returns (database, connection). Caller is responsible for `del`-ing both
    to release the Kùzu file lock — Connection has no explicit close() on
    Python 0.11.
    """
    import kuzu  # type: ignore

    path = board_kuzu_path(board_id)
    if not path.exists():
        # Bootstrap on-demand so callers never see a missing file.
        bootstrap_board_graph(board_id)
    db = kuzu.Database(str(path))
    conn = kuzu.Connection(db)
    load_vector_extension(conn)
    return db, conn


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
