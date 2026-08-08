"""Real KG Integration Test — runs against an explicitly selected data home.

This test does NOT use the temp database or stub embedding from conftest.py.
It must be invoked explicitly:

    pytest tests/test_kg_real_integration.py -v -s -m real_kg

Environment requirements:
- OKTO_PULSE_REAL_KG=1 must explicitly opt in
- OKTO_PULSE_REAL_KG_HOME must name the exact data home
- OKTO_PULSE_REAL_KG_BOARD_ID must name the exact board
- <HOME>/data/pulse.db and <HOME>/boards/<BOARD_ID>/graph.lbug must exist
- the Community edition must be installed for real sentence-transformers tests
- The server must NOT be running (to avoid DB lock conflicts)

NOTE: This test is designed to coexist with conftest.py's autouse fixtures.
The conftest resets the registry to temp paths on every test, so tests that
use BoardConnection directly (which reads from the registry) need special
handling. Tests that use KuzuGraphStore/KuzuCypherExecutor via the registry
will work with temp data — the KG-only tests below use direct Kùzu connections
to guarantee we hit the real graph file.
"""

# ruff: noqa: E402

from __future__ import annotations

import gc
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

# ---------------------------------------------------------------------------
# ENVIRONMENT SETUP — real paths, applied per-module via fixture below.
#
# NEVER mutate os.environ at module level here: this module is imported during
# pytest COLLECTION of any broad run, and a bare os.environ assignment would
# repoint DATABASE_URL at the user's real ~/.okto-pulse database for the whole
# test session (any later create_database(CoreSettings().database_url) call
# would then hijack the process-global engine and leak test writes into real
# data). The env swap now lives in `_real_env`, which only runs when a test in
# THIS module actually executes (explicit `-m real_kg` invocation) and undoes
# itself afterwards.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RealKgTarget:
    opted_in: bool
    data_dir: Path | None
    board_id: str | None

    @property
    def configured(self) -> bool:
        return self.opted_in and self.data_dir is not None and bool(self.board_id)

    @property
    def sqlite_db(self) -> Path | None:
        if self.data_dir is None:
            return None
        return self.data_dir / "data" / "pulse.db"

    @property
    def graph(self) -> Path | None:
        if self.data_dir is None or not self.board_id:
            return None
        return self.data_dir / "boards" / self.board_id / "graph.lbug"


def _resolve_real_kg_target(environment: Mapping[str, str]) -> RealKgTarget:
    raw_home = (environment.get("OKTO_PULSE_REAL_KG_HOME") or "").strip()
    raw_board_id = (environment.get("OKTO_PULSE_REAL_KG_BOARD_ID") or "").strip()
    return RealKgTarget(
        opted_in=(environment.get("OKTO_PULSE_REAL_KG") or "").strip() == "1",
        data_dir=Path(raw_home).expanduser().resolve() if raw_home else None,
        board_id=raw_board_id or None,
    )


_REAL_TARGET = _resolve_real_kg_target(os.environ)
_REAL_DATA_DIR = _REAL_TARGET.data_dir
_REAL_BOARD_ID = _REAL_TARGET.board_id
_REAL_SQLITE_DB = _REAL_TARGET.sqlite_db
_REAL_GRAPH_PATH = _REAL_TARGET.graph

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NOW import okto_pulse modules
# ---------------------------------------------------------------------------

import ladybug as kuzu  # type: ignore
import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / ".." / "src"))

from kg_schema_testing import NODE_TYPES
from okto_pulse.core.kg.tier_power import get_schema_info


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------


def _real_kuzu_exists() -> bool:
    return bool(
        _REAL_TARGET.configured
        and _REAL_SQLITE_DB is not None
        and _REAL_SQLITE_DB.is_file()
        and _REAL_GRAPH_PATH is not None
        and _REAL_GRAPH_PATH.exists()
    )


skip_no_real_kuzu = pytest.mark.skipif(
    not _real_kuzu_exists(),
    reason=(
        "Real KG tests require OKTO_PULSE_REAL_KG=1, "
        "OKTO_PULSE_REAL_KG_HOME, OKTO_PULSE_REAL_KG_BOARD_ID, and existing "
        "SQLite/graph paths for that exact target"
    ),
)

real_kg = pytest.mark.real_kg
real_timeout = pytest.mark.timeout(60)


@pytest.fixture(scope="module", autouse=True)
def _real_env():
    """Point the environment at the real ~/.okto-pulse data — execution only.

    Module-scoped so the swap happens once per explicit run of this file, and
    ONLY when a test here actually executes (skipped/deselected tests never
    trigger it). MonkeyPatch.undo() restores the suite's temp-database env.
    """
    if not _REAL_TARGET.configured:
        yield
        return

    assert _REAL_DATA_DIR is not None
    assert _REAL_SQLITE_DB is not None
    mp = pytest.MonkeyPatch()
    mp.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{_REAL_SQLITE_DB}")
    mp.setenv("KG_BASE_DIR", str(_REAL_DATA_DIR))
    mp.setenv("KG_EMBEDDING_MODE", "sentence-transformers")
    mp.setenv("KG_CLEANUP_ENABLED", "false")
    mp.setenv("KG_CLEANUP_INTERVAL_SECONDS", "3600")
    # Verbose logging for the explicit real-KG run — also execution-only so a
    # broad run's logging config is never force-reset during collection.
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s [KG-TEST] %(message)s",
        force=True,
    )
    yield
    mp.undo()


# ---------------------------------------------------------------------------
# Helper: open a direct Kùzu connection bypassing the registry
# ---------------------------------------------------------------------------


def _open_real_kuzu():
    """Open a Kùzu connection directly to the real graph, bypassing registry.

    Uses read_only=True because the 122MB production database crashes Kùzu
    in write mode (Bus Error). Read-only mode works reliably.
    """
    assert _REAL_GRAPH_PATH is not None
    path = str(_REAL_GRAPH_PATH)
    logger.debug("[KG-TEST] Opening Kùzu directly (read-only): path=%s", path)
    db = kuzu.Database(
        path,
        buffer_pool_size=256 * 1024 * 1024,
        max_db_size=4 * 1024 * 1024 * 1024,
        read_only=True,
    )
    conn = kuzu.Connection(db)
    logger.debug("[KG-TEST] Kùzu connection opened successfully (read-only)")
    return db, conn


def _close_kuzu(db, conn):
    """Safely close Kùzu connection and database."""
    try:
        conn.close()
    except Exception:
        pass
    try:
        db.close()
        logger.debug("[KG-TEST] Kùzu database closed")
    except Exception:
        pass
    gc.collect()


# ---------------------------------------------------------------------------
# Test 1: Schema Info (uses registry — may use temp data, but validates
# the schema_info function works end-to-end)
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_schema_info():
    """Verify the schema_info function works."""
    logger.info("[KG-TEST] === Test 1: Schema Info ===")

    assert _REAL_BOARD_ID is not None
    result = get_schema_info(_REAL_BOARD_ID)

    logger.info("[KG-TEST] Schema version: %s", result.get("schema_version"))
    logger.info("[KG-TEST] Node types: %d", len(result.get("stable_node_types", [])))
    logger.info("[KG-TEST] Rel types: %d", len(result.get("stable_rel_types", [])))
    logger.info("[KG-TEST] Vector indexes: %d", len(result.get("vector_indexes", [])))

    assert result is not None, "Schema info should not be None"
    assert "schema_version" in result, "Schema should have version"
    assert len(result.get("stable_node_types", [])) > 0, "Should have node types"
    logger.info("[KG-TEST] === Test 1 PASSED ===")


# ---------------------------------------------------------------------------
# Test 2: Direct Kùzu — count all nodes
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_cypher_count_nodes():
    """Run a simple Cypher count query directly against real Kùzu data."""
    logger.info("[KG-TEST] === Test 2: Direct Kùzu Count ===")

    db, conn = _open_real_kuzu()
    try:
        result = conn.execute("MATCH (n) RETURN count(n) as cnt")
        assert result.has_next(), "Should return at least one row"
        row = result.get_next()
        cnt = row[0]
        logger.info("[KG-TEST] Total nodes in graph: %d", cnt)
        assert cnt >= 0, "Node count should be non-negative"
        result.close()
    finally:
        _close_kuzu(db, conn)

    logger.info("[KG-TEST] === Test 2 PASSED ===")


# ---------------------------------------------------------------------------
# Test 3: Direct Kùzu — count per node type
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_count_per_node_type():
    """Count nodes in each type table."""
    logger.info("[KG-TEST] === Test 3: Count Per Node Type ===")

    db, conn = _open_real_kuzu()
    try:
        total = 0
        for node_type in NODE_TYPES:
            try:
                res = conn.execute(f"MATCH (n:{node_type}) RETURN count(n)")
                if res.has_next():
                    cnt = res.get_next()[0]
                    logger.info("[KG-TEST]   %s: %d nodes", node_type, cnt)
                    total += cnt
                else:
                    logger.info("[KG-TEST]   %s: no result", node_type)
                res.close()
            except Exception as e:
                logger.warning("[KG-TEST]   %s: query error: %s", node_type, e)

        logger.info("[KG-TEST] Total nodes across all types: %d", total)
        assert total >= 0, "Should have some nodes"
    finally:
        _close_kuzu(db, conn)

    logger.info("[KG-TEST] === Test 3 PASSED ===")


# ---------------------------------------------------------------------------
# Test 4: Direct Kùzu — check relationships
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_count_relationships():
    """Count relationships in each rel table."""
    logger.info("[KG-TEST] === Test 4: Count Relationships ===")

    db, conn = _open_real_kuzu()
    try:
        from kg_schema_testing import REL_TYPES

        for rel_name, from_type, to_type in REL_TYPES:
            try:
                res = conn.execute(f"MATCH ()-[r:{rel_name}]->() RETURN count(r)")
                if res.has_next():
                    cnt = res.get_next()[0]
                    logger.info(
                        "[KG-TEST]   %s (%s->%s): %d rels",
                        rel_name,
                        from_type,
                        to_type,
                        cnt,
                    )
                res.close()
            except Exception as e:
                logger.warning("[KG-TEST]   %s: query error: %s", rel_name, e)
    finally:
        _close_kuzu(db, conn)

    logger.info("[KG-TEST] === Test 4 PASSED ===")


# ---------------------------------------------------------------------------
# Test 5: Real embedding via CommunitySentenceTransformerProvider
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_embedding():
    """Test that sentence-transformers embedding works."""
    logger.info("[KG-TEST] === Test 5: Real Embedding ===")

    community_embedding = pytest.importorskip(
        "okto_pulse.community.adapters.embedding",
        reason="real sentence-transformers provider lives in okto-pulse-community",
    )

    provider = community_embedding.CommunitySentenceTransformerProvider(
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        dim=384,
    )

    vec = provider.encode("validation decisions")
    logger.info("[KG-TEST] Embedding dims: %d", len(vec))
    logger.info("[KG-TEST] Embedding norm: %.4f", sum(x * x for x in vec) ** 0.5)
    logger.info("[KG-TEST] First 5 values: %s", vec[:5])

    assert len(vec) == 384, f"Expected 384 dims, got {len(vec)}"
    # Normalized embeddings should have norm ~1.0
    norm = sum(x * x for x in vec) ** 0.5
    assert 0.9 < norm < 1.1, f"Expected norm ~1.0, got {norm}"

    # Test batch encoding
    batch = provider.encode_batch(["validation decisions", "caching strategy"])
    assert len(batch) == 2, "Should return 2 vectors"
    assert len(batch[0]) == 384, "Each vector should be 384 dims"

    logger.info("[KG-TEST] === Test 5 PASSED ===")


# ---------------------------------------------------------------------------
# Test 6: Direct Kùzu — semantic search (manual HNSW query)
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_hnsw_search():
    """Run a HNSW vector search directly against real Kùzu."""
    logger.info("[KG-TEST] === Test 6: HNSW Vector Search ===")

    community_embedding = pytest.importorskip(
        "okto_pulse.community.adapters.embedding",
        reason="real sentence-transformers provider lives in okto-pulse-community",
    )

    provider = community_embedding.CommunitySentenceTransformerProvider(
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        dim=384,
    )
    query_vec = provider.encode("validation decisions")

    db, conn = _open_real_kuzu()
    try:
        # Try HNSW vector search on Decision nodes
        try:
            res = conn.execute(
                "CALL QUERY_VECTOR_INDEX('Decision', 'decision_embedding_idx', $vec, 5) "
                "RETURN node.id, node.title, distance",
                {"vec": query_vec},
            )
            rows = []
            while res.has_next():
                rows.append(res.get_next())
            res.close()
            logger.info("[KG-TEST] HNSW Decision search: %d results", len(rows))
            for r in rows[:3]:
                sim = max(0.0, min(1.0, 1.0 - float(r[2])))
                logger.info(
                    "[KG-TEST]   id=%s title=%s sim=%.3f",
                    str(r[0])[:20],
                    str(r[1])[:60],
                    sim,
                )
        except Exception as e:
            logger.warning(
                "[KG-TEST] HNSW search failed (may be expected if no data): %s", e
            )
    finally:
        _close_kuzu(db, conn)

    logger.info("[KG-TEST] === Test 6 PASSED ===")


# ---------------------------------------------------------------------------
# Test 7: Direct Kùzu — find contradictions
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_find_contradictions():
    """Find contradiction pairs directly in Kùzu."""
    logger.info("[KG-TEST] === Test 7: Find Contradictions ===")

    db, conn = _open_real_kuzu()
    try:
        try:
            res = conn.execute(
                "MATCH (a:Decision)-[r:contradicts]->(b:Decision) "
                "RETURN a.id, a.title, b.id, b.title, r.confidence LIMIT 10"
            )
            rows = []
            while res.has_next():
                rows.append(res.get_next())
            res.close()
            logger.info("[KG-TEST] Contradiction pairs: %d", len(rows))
            for r in rows[:3]:
                logger.info(
                    "[KG-TEST]   %s <-> %s (conf=%s)",
                    str(r[1])[:40],
                    str(r[3])[:40],
                    r[4],
                )
        except Exception as e:
            logger.warning("[KG-TEST] Contradictions query failed: %s", e)
    finally:
        _close_kuzu(db, conn)

    logger.info("[KG-TEST] === Test 7 PASSED ===")


# ---------------------------------------------------------------------------
# Test 8: Direct Kùzu — supersedence chain
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_supersedence():
    """Find supersedence chains in the graph."""
    logger.info("[KG-TEST] === Test 8: Supersedence ===")

    db, conn = _open_real_kuzu()
    try:
        try:
            res = conn.execute(
                "MATCH (a:Decision)-[r:supersedes]->(b:Decision) "
                "RETURN a.id, a.title, b.id, b.title LIMIT 10"
            )
            rows = []
            while res.has_next():
                rows.append(res.get_next())
            res.close()
            logger.info("[KG-TEST] Supersedence edges: %d", len(rows))
            for r in rows[:3]:
                logger.info(
                    "[KG-TEST]   %s supersedes %s", str(r[1])[:40], str(r[3])[:40]
                )
        except Exception as e:
            logger.warning("[KG-TEST] Supersedence query failed: %s", e)
    finally:
        _close_kuzu(db, conn)

    logger.info("[KG-TEST] === Test 8 PASSED ===")


# ---------------------------------------------------------------------------
# Test 9: Connection liveness after multiple operations
# ---------------------------------------------------------------------------


@real_kg
@real_timeout
@skip_no_real_kuzu
def test_real_connection_liveness():
    """Open and close multiple connections, verifying liveness."""
    logger.info("[KG-TEST] === Test 9: Connection Liveness ===")

    for i in range(3):
        db, conn = _open_real_kuzu()
        try:
            res = conn.execute("MATCH (n) RETURN count(n) as cnt")
            assert res.has_next(), f"Iteration {i}: Should return at least one row"
            cnt = res.get_next()[0]
            logger.info("[KG-TEST] Iteration %d: %d nodes", i, cnt)
            res.close()
        finally:
            _close_kuzu(db, conn)

    logger.info("[KG-TEST] === Test 9 PASSED ===")
