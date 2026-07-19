"""Topic clustering + Entity canonicalization for the global discovery layer.

Topic clustering: cosine similarity > 0.75 → assign to existing topic (update
centroid via weighted average). Below threshold → create new topic with TF-IDF
heuristic name from title keywords.

Entity canonicalization: combined_score = 0.6*semantic + 0.3*string_fuzzy +
0.1*alias_match. Merge when combined > 0.85.

Board delete cascade: remove Board node + orphan digests + decrement counts on
Topics/Entities, GC those with zero references.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import threading
import unicodedata
import uuid
from contextvars import copy_context
from typing import Any

logger = logging.getLogger("okto_pulse.kg.global_discovery.clustering")

TOPIC_SIMILARITY_THRESHOLD = 0.75
ENTITY_CANONICALIZATION_THRESHOLD = 0.85


def _run_async_blocking(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: dict[str, Any] = {}

    def _runner() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - re-raised below
            box["error"] = exc

    context = copy_context()
    thread = threading.Thread(
        target=context.run,
        args=(_runner,),
        name="kg-global-discovery-board-write",
        daemon=True,
    )
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")


def _execute_board_write_sync(
    board_id: str,
    cypher: str,
    params: dict[str, Any] | None = None,
) -> Any:
    from okto_pulse.core.kg.interfaces import get_kg_registry

    async def _run() -> Any:
        async with await get_kg_registry().graph_transaction.begin(board_id) as scope:
            return scope.execute(cypher, params or {})

    return _run_async_blocking(_run())


def _first_count(result: Any) -> int:
    rows = getattr(result, "rows", ()) if result is not None else ()
    if rows:
        return int(rows[0][0] or 0)
    return 0


def normalize_name(name: str) -> str:
    """NFKD + strip punctuation + lowercase for fuzzy matching."""
    normalized = unicodedata.normalize("NFKD", name)
    normalized = re.sub(r"[^\w\s]", "", normalized)
    return normalized.lower().strip()


def string_fuzzy_ratio(a: str, b: str) -> float:
    """Simple character-level similarity (Levenshtein-free). Uses set
    intersection of trigrams as a fast approximation."""
    if not a or not b:
        return 0.0
    a_norm = normalize_name(a)
    b_norm = normalize_name(b)
    if a_norm == b_norm:
        return 1.0
    a_tri = {a_norm[i:i+3] for i in range(len(a_norm) - 2)}
    b_tri = {b_norm[i:i+3] for i in range(len(b_norm) - 2)}
    if not a_tri or not b_tri:
        return 0.0
    return len(a_tri & b_tri) / max(len(a_tri), len(b_tri))


def cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def entity_combined_score(
    semantic: float,
    string_fuzzy: float,
    alias_match: float,
) -> float:
    return 0.6 * semantic + 0.3 * string_fuzzy + 0.1 * alias_match


def board_delete_cascade(board_id: str) -> dict:
    """Remove a board and cascade-cleanup orphans from the global discovery.

    Also wipes the per-board graph backend graph (every node + edge) so that re-
    running historical consolidation rebuilds from a clean slate. Without
    this step the global cleanup leaves orphan nodes from prior workers
    (e.g. legacy `FR-{n}` Requirement nodes) that nothing references —
    polluting the canvas with disconnected debris.

    Returns counts of removed entities per type.

    KG-01.3.1 boundary: this is a destructive write path against both
    board graph and global graph. require_write_token() raises in STRICT
    mode if no safe-write guard is active; in SOFT mode logs + bumps
    kg_unguarded_write_total. Production wires the caller (admin tooling
    or rebuild service) to enter KGSafeWriteLifecycle under the admin
    lane first.
    """
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.schema_contract import NODE_TYPES
    from okto_pulse.core.kg.write_barrier import require_write_token

    require_write_token(board_id)

    counts = {
        "board_removed": False,
        "digests_removed": 0,
        "topics_decremented": 0,
        "entities_decremented": 0,
        "board_nodes_removed": 0,
    }

    # 0. Wipe per-board SQLite audit + outbox + queue rows. Without this the
    # next consolidation re-uses the stale `content_hash` from a prior commit
    # and `propose_reconciliation` short-circuits every candidate to NOOP —
    # the queue worker reports "done" but graph backend stays empty.
    try:
        from okto_pulse.core.ports.board_relational_cleanup import (
            get_board_relational_cleanup_port,
        )

        sqlite_counts = _run_async_blocking(
            get_board_relational_cleanup_port().wipe_runtime_rows(
                board_id=board_id
            )
        )
        for k, v in sqlite_counts.items():
            counts[f"sqlite_{k}_removed"] = v
    except Exception as exc:
        logger.warning(
            "board_delete.sqlite_wipe_failed board=%s err=%s",
            board_id, exc,
        )

    # 1. Wipe per-board graph backend graph (skip BoardMeta singleton).
    try:
        if get_kg_registry().graph_runtime_store.exists(board_id):
            for node_type in NODE_TYPES:
                if node_type == "BoardMeta":
                    continue
                try:
                    result = _execute_board_write_sync(
                        board_id,
                        f"MATCH (n:{node_type}) DETACH DELETE n RETURN count(n)",
                    )
                    counts["board_nodes_removed"] += _first_count(result)
                except Exception as exc:
                    logger.warning(
                        "board_delete.per_board_wipe_failed "
                        "board=%s type=%s err=%s",
                        board_id, node_type, exc,
                    )
    except Exception as exc:
        logger.warning(
            "board_delete.per_board_open_failed board=%s err=%s",
            board_id, exc,
        )

    from okto_pulse.core.kg.global_discovery_writer import (
        global_discovery_writer_scope,
    )

    global_runtime = get_kg_registry().require_global_discovery_runtime()
    with global_discovery_writer_scope(
        operation="board_delete_cascade.global",
        owner_id=f"board-delete:{board_id}",
        admin_lane=True,
    ):
        # Delete DecisionDigests for this board
        result = global_runtime.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $bid "
            "DETACH DELETE d RETURN count(d)",
            {"bid": board_id},
        )
        if result.rows:
            counts["digests_removed"] = result.rows[0][0]

        # Delete Board node + edges
        global_runtime.execute(
            "MATCH (b:Board {board_id: $bid}) DETACH DELETE b",
            {"bid": board_id},
        )
        counts["board_removed"] = True

        # GC orphan Topics (member_count = 0 or no edges)
        try:
            global_runtime.execute(
                "MATCH (t:Topic) WHERE NOT EXISTS { MATCH ()-[:HAS_TOPIC]->(t) } "
                "DETACH DELETE t"
            )
        except Exception:
            pass

        # GC orphan Entities (no edges)
        try:
            global_runtime.execute(
                "MATCH (e:Entity) WHERE NOT EXISTS { MATCH ()-[:MENTIONS_ENTITY]->(e) } "
                "AND NOT EXISTS { MATCH ()-[:DECISION_MENTIONS_ENTITY]->(e) } "
                "DETACH DELETE e"
            )
        except Exception:
            pass

    logger.info(
        "global.cascade board=%s digests=%d",
        board_id, counts["digests_removed"],
        extra={"event": "global.cascade", "board_id": board_id, **counts},
    )
    return counts


def gc_orphans(*, dry_run: bool = True, entity_age_days: int = 90) -> dict:
    """Garbage collect orphan Topics and Entities from the global graph.

    Even in ``dry_run=True`` mode the embedded engine may mutate its WAL while
    opening a connection.  This function therefore acquires the same durable
    cross-process fence as recovery and outbox writers; callers never mint a
    context-only token.
    """
    from okto_pulse.core.kg.global_discovery_writer import (
        global_discovery_writer_scope,
    )
    from okto_pulse.core.kg.interfaces import get_kg_registry
    counts = {"topics_removed": 0, "entities_removed": 0, "dry_run": dry_run}

    global_runtime = get_kg_registry().require_global_discovery_runtime()
    try:
        with global_discovery_writer_scope(
            operation="gc_orphans",
            owner_id=f"global-gc:{uuid.uuid4().hex[:12]}",
            admin_lane=not dry_run,
        ):
            # Count orphan topics
            r = global_runtime.execute(
                "MATCH (t:Topic) WHERE NOT EXISTS { MATCH ()-[:HAS_TOPIC]->(t) } "
                "RETURN count(t)"
            )
            orphan_topics = r.rows[0][0] if r.rows else 0
            counts["topics_removed"] = orphan_topics

            r = global_runtime.execute(
                "MATCH (e:Entity) WHERE NOT EXISTS { MATCH ()-[:MENTIONS_ENTITY]->(e) } "
                "AND NOT EXISTS { MATCH ()-[:DECISION_MENTIONS_ENTITY]->(e) } "
                "RETURN count(e)"
            )
            orphan_entities = r.rows[0][0] if r.rows else 0
            counts["entities_removed"] = orphan_entities

            if not dry_run:
                global_runtime.execute(
                    "MATCH (t:Topic) WHERE NOT EXISTS { MATCH ()-[:HAS_TOPIC]->(t) } "
                    "DETACH DELETE t"
                )
                global_runtime.execute(
                    "MATCH (e:Entity) WHERE NOT EXISTS { MATCH ()-[:MENTIONS_ENTITY]->(e) } "
                    "AND NOT EXISTS { MATCH ()-[:DECISION_MENTIONS_ENTITY]->(e) } "
                    "DETACH DELETE e"
                )
    except Exception as exc:
        logger.error("gc_orphans.error err=%s", exc)

    logger.info(
        "global.gc dry_run=%s topics=%d entities=%d",
        dry_run, counts["topics_removed"], counts["entities_removed"],
        extra={"event": "global.gc", **counts},
    )
    return counts


def rebuild_from_scratch(board_ids: list[str] | None = None) -> dict:
    """Drop and rebuild the global discovery meta-graph.

    If board_ids is None, rebuilds for all boards with existing graph files.

    KG-01.4 (val_b15ff42f rework): the drop of `global graph` and
    sidecars MUST go through ``purge_global_discovery_storage`` so the
    files are quarantined with a manifest before being moved out of the
    way. Direct ``unlink``/``rmtree`` is forbidden by FR7/AC10/IR
    ``ir_f175bc42``. We enter ``under_global_safe_write`` so the
    purge's barrier check + the bootstrap's barrier check both pass
    inside one logical "rebuild" operation.

    The previous ad-hoc ``discovery_backup_<hex>`` copy is dropped —
    quarantine IS the durable evidence path now, with a manifest that
    the operator can audit (TR8 fields), retention enforced (TR9), and
    counter-instrumented. A separate "backup" without a manifest only
    confused responders.
    """
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.global_discovery_writer import (
        global_discovery_writer_scope,
    )

    global_runtime = get_kg_registry().require_global_discovery_runtime()
    purge_response = None
    rebuild_token = f"rebuild-{uuid.uuid4().hex[:12]}"

    with global_discovery_writer_scope(
        operation="rebuild_from_scratch",
        owner_id=rebuild_token,
        admin_lane=True,
    ):
        if global_runtime.state().exists:
            purge_response = global_runtime.purge(reason="rebuild_from_scratch")
        global_runtime.bootstrap()

    return {
        "status": "rebuilt",
        "quarantined_storage_refs": (
            ["global-discovery"]
            if purge_response is not None and purge_response.removed
            else []
        ),
    }
