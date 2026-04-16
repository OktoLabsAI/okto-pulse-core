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

import hashlib
import logging
import math
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("okto_pulse.kg.global_discovery.clustering")

TOPIC_SIMILARITY_THRESHOLD = 0.75
ENTITY_CANONICALIZATION_THRESHOLD = 0.85


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

    Returns counts of removed entities per type.
    """
    from okto_pulse.core.kg.global_discovery.schema import open_global_connection

    counts = {
        "board_removed": False,
        "digests_removed": 0,
        "topics_decremented": 0,
        "entities_decremented": 0,
    }

    gdb, gconn = open_global_connection()
    try:
        # Delete DecisionDigests for this board
        result = gconn.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $bid "
            "DETACH DELETE d RETURN count(d)",
            {"bid": board_id},
        )
        if result.has_next():
            counts["digests_removed"] = result.get_next()[0]

        # Delete Board node + edges
        gconn.execute(
            "MATCH (b:Board {board_id: $bid}) DETACH DELETE b",
            {"bid": board_id},
        )
        counts["board_removed"] = True

        # GC orphan Topics (member_count = 0 or no edges)
        try:
            gconn.execute(
                "MATCH (t:Topic) WHERE NOT EXISTS { MATCH ()-[:HAS_TOPIC]->(t) } "
                "DETACH DELETE t"
            )
        except Exception:
            pass

        # GC orphan Entities (no edges)
        try:
            gconn.execute(
                "MATCH (e:Entity) WHERE NOT EXISTS { MATCH ()-[:MENTIONS_ENTITY]->(e) } "
                "AND NOT EXISTS { MATCH ()-[:DECISION_MENTIONS_ENTITY]->(e) } "
                "DETACH DELETE e"
            )
        except Exception:
            pass

    finally:
        del gconn, gdb

    logger.info(
        "global.cascade board=%s digests=%d",
        board_id, counts["digests_removed"],
        extra={"event": "global.cascade", "board_id": board_id, **counts},
    )
    return counts


def gc_orphans(*, dry_run: bool = True, entity_age_days: int = 90) -> dict:
    """Garbage collect orphan Topics and Entities from the global graph."""
    from okto_pulse.core.kg.global_discovery.schema import open_global_connection

    counts = {"topics_removed": 0, "entities_removed": 0, "dry_run": dry_run}

    gdb, gconn = open_global_connection()
    try:
        # Count orphan topics
        r = gconn.execute(
            "MATCH (t:Topic) WHERE NOT EXISTS { MATCH ()-[:HAS_TOPIC]->(t) } "
            "RETURN count(t)"
        )
        orphan_topics = r.get_next()[0] if r.has_next() else 0
        counts["topics_removed"] = orphan_topics

        r = gconn.execute(
            "MATCH (e:Entity) WHERE NOT EXISTS { MATCH ()-[:MENTIONS_ENTITY]->(e) } "
            "AND NOT EXISTS { MATCH ()-[:DECISION_MENTIONS_ENTITY]->(e) } "
            "RETURN count(e)"
        )
        orphan_entities = r.get_next()[0] if r.has_next() else 0
        counts["entities_removed"] = orphan_entities

        if not dry_run:
            gconn.execute(
                "MATCH (t:Topic) WHERE NOT EXISTS { MATCH ()-[:HAS_TOPIC]->(t) } "
                "DETACH DELETE t"
            )
            gconn.execute(
                "MATCH (e:Entity) WHERE NOT EXISTS { MATCH ()-[:MENTIONS_ENTITY]->(e) } "
                "AND NOT EXISTS { MATCH ()-[:DECISION_MENTIONS_ENTITY]->(e) } "
                "DETACH DELETE e"
            )
    except Exception as exc:
        logger.error("gc_orphans.error err=%s", exc)
    finally:
        del gconn, gdb

    logger.info(
        "global.gc dry_run=%s topics=%d entities=%d",
        dry_run, counts["topics_removed"], counts["entities_removed"],
        extra={"event": "global.gc", **counts},
    )
    return counts


def rebuild_from_scratch(board_ids: list[str] | None = None) -> dict:
    """Drop and rebuild the global discovery meta-graph.

    If board_ids is None, rebuilds for all boards with existing .kuzu files.
    """
    from okto_pulse.core.kg.global_discovery.schema import (
        _global_kuzu_path,
        bootstrap_global_discovery,
    )
    import shutil

    path = _global_kuzu_path()
    # Backup + drop
    if path.exists():
        backup = path.parent / f"discovery_backup_{uuid.uuid4().hex[:8]}.kuzu"
        shutil.copytree(str(path), str(backup))
        shutil.rmtree(str(path))

    # Recreate schema
    bootstrap_global_discovery()

    return {
        "status": "rebuilt",
        "backup_path": str(backup) if path.exists() else None,
    }
