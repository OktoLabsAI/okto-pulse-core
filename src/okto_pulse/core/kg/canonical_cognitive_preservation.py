"""R2-IMP2 — preserve canonical cognitive knowledge across a board rebuild.

A deterministic rebuild purges the board Kùzu graph and re-materializes only
SQL-derived deterministic nodes. Canonical COGNITIVE nodes (Learning / Alternative
/ Assumption of cognitive origin) have no SQL source, so without preservation they
are SILENTLY LOST. This module snapshots them (node attrs + relevant edges) BEFORE
the purge and restores them AFTER deterministic re-materialization. Anything that
cannot be literally restored is reported as ``unrestorable`` so the rebuild can be
flagged ``degraded`` (never a silent clean success), and bug-derived Learnings get
a traceable R7 cognitive hold.

Outcome semantics (R2-IMP2 / codex gate):
- ``clean``           — everything snapshotted was restored literally;
- ``degraded``        — some cognitive canonical was lost but is traceable
                        (recorded in the report summary + R7 hold where it fits);
- ``unreadable``      — the pre-purge graph could not be read; nothing could be
                        snapshotted, recorded as ``readable=False`` (NOT a silent
                        ``snapshotted=0`` success);
- ``integrity_error`` — the preservation mechanism itself failed to produce a
                        trace (the caller fails the rebuild closed).

NOT an MCP mutating tool. Global Discovery digest sync is R2-IMP5, not here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("okto_pulse.kg.canonical_cognitive_preservation")

COGNITIVE_TYPES: tuple[str, ...] = ("Learning", "Alternative", "Assumption")
GRAPH_LAYER_CANONICAL = "canonical"

# Outcome status vocabulary (structured report field, NOT a persisted enum).
STATUS_CLEAN = "clean"
STATUS_DEGRADED = "degraded"
STATUS_UNREADABLE = "unreadable"
STATUS_INTEGRITY_ERROR = "integrity_error"

# Semantic/provenance rel types a cognitive node may carry. We snapshot the
# cognitive node's OUTGOING edges of these types; restore only re-creates an edge
# when BOTH endpoints exist in the rebuilt graph and the rel can be created by the
# current contract (failures -> unrestorable, never a guard bypass).
_COGNITIVE_EDGE_TYPES: tuple[str, ...] = (
    "validates", "derives_from", "relates_to", "supersedes", "contradicts",
    "depends_on", "mentions",
)


@dataclass
class CognitiveSnapshot:
    board_id: str
    readable: bool
    nodes: list[dict[str, Any]] = field(default_factory=list)  # {node_type, id, attrs}
    edges: list[dict[str, Any]] = field(default_factory=list)  # {edge_type, from_id, to_id}

    @property
    def node_count(self) -> int:
        return len(self.nodes)


@dataclass
class RestoreResult:
    restored_nodes: int = 0
    restored_edges: int = 0
    unrestorable: list[dict[str, Any]] = field(default_factory=list)
    unreadable: bool = False
    error: str | None = None


def _table_columns(conn, node_type: str) -> list[str]:
    cols: list[str] = []
    try:
        res = conn.execute(f"CALL TABLE_INFO('{node_type}') RETURN *")
        while res.has_next():
            row = res.get_next()
            for cell in row:
                if isinstance(cell, str) and cell:
                    cols.append(cell)
                    break
    except Exception:
        return []
    return cols


def snapshot_canonical_cognitive(board_id: str) -> CognitiveSnapshot:
    """Read canonical cognitive nodes + their outgoing semantic edges BEFORE the
    rebuild purge. Degrades to ``readable=False`` (auditable) if the graph cannot
    be opened/read — never claims an empty snapshot as a clean success."""
    from okto_pulse.core.kg.schema import open_board_connection

    try:
        with open_board_connection(board_id) as (_db, conn):
            nodes: list[dict[str, Any]] = []
            node_ids: set[str] = set()
            for ntype in COGNITIVE_TYPES:
                cols = _table_columns(conn, ntype)
                if not cols:
                    continue
                projection = ", ".join(f"n.{c}" for c in cols)
                res = conn.execute(
                    f"MATCH (n:{ntype}) WHERE n.graph_layer = $c RETURN {projection}",
                    {"c": GRAPH_LAYER_CANONICAL},
                )
                while res.has_next():
                    row = res.get_next()
                    attrs = {cols[i]: row[i] for i in range(len(cols))}
                    nid = str(attrs.get("id") or "")
                    if not nid:
                        continue
                    node_ids.add(nid)
                    nodes.append({"node_type": ntype, "id": nid, "attrs": attrs})
            edges = _snapshot_edges(conn, node_ids) if node_ids else []
        return CognitiveSnapshot(board_id=board_id, readable=True, nodes=nodes, edges=edges)
    except Exception as exc:
        logger.warning(
            "kg.cognitive_preservation.snapshot_unreadable board=%s err=%s",
            board_id, exc,
            extra={"event": "kg.cognitive_preservation.snapshot_unreadable",
                   "board_id": board_id},
        )
        return CognitiveSnapshot(board_id=board_id, readable=False)


def _snapshot_edges(conn, node_ids: set[str]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    ids = list(node_ids)
    for etype in _COGNITIVE_EDGE_TYPES:
        try:
            res = conn.execute(
                f"MATCH (a)-[:{etype}]->(b) WHERE a.id IN $ids "
                f"RETURN a.id, b.id",
                {"ids": ids},
            )
        except Exception:
            continue
        while res.has_next():
            row = res.get_next()
            from_id, to_id = str(row[0]), str(row[1])
            key = (etype, from_id, to_id)
            if key in seen:
                continue
            seen.add(key)
            edges.append({"edge_type": etype, "from_id": from_id, "to_id": to_id})
    return edges


def _node_present(conn, node_type: str, node_id: str) -> bool:
    res = conn.execute(
        f"MATCH (n:{node_type} {{id: $id}}) RETURN n.id", {"id": node_id}
    )
    return res.has_next()


def _node_label(conn, node_id: str) -> str | None:
    """Resolve the label of an existing node id in the rebuilt graph (for edge
    endpoint typing). Returns None if absent."""
    from okto_pulse.core.kg.schema import VECTOR_INDEX_TYPES

    candidates = tuple(VECTOR_INDEX_TYPES) + ("Alternative", "Assumption")
    for label in candidates:
        try:
            res = conn.execute(
                f"MATCH (n:{label} {{id: $id}}) RETURN n.id", {"id": node_id}
            )
            if res.has_next():
                return label
        except Exception:
            continue
    return None


def restore_canonical_cognitive(board_id: str, snapshot: CognitiveSnapshot) -> RestoreResult:
    """Re-create snapshotted cognitive nodes + restorable edges AFTER the rebuild
    re-materialized deterministic nodes. Idempotent (skips nodes already present).
    Edges are restored only when BOTH endpoints exist and the rel can be created;
    otherwise the edge/node is recorded ``unrestorable`` (never a guard bypass)."""
    from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator
    import uuid as _uuid

    if not snapshot.readable:
        return RestoreResult(unreadable=True)
    if not snapshot.nodes:
        return RestoreResult()  # nothing canonical-cognitive to preserve (clean)

    result = RestoreResult()
    try:
        with open_board_connection(board_id) as (_db, conn):
            orch = TransactionOrchestrator(
                kuzu_conn=conn, sqlite_session=None,
                session_id=f"cogrestore_{_uuid.uuid4().hex[:8]}", board_id=board_id,
            )
            node_by_id = {n["id"]: n for n in snapshot.nodes}
            present: set[str] = set()
            for node in snapshot.nodes:
                ntype, nid = node["node_type"], node["id"]
                if _node_present(conn, ntype, nid):
                    present.add(nid)
                    continue  # idempotent — survived / already restored
                attrs = {k: v for k, v in node["attrs"].items() if k != "id"}
                try:
                    _apply_kuzu_node_create_with_timestamp(orch, ntype, nid, attrs)
                    result.restored_nodes += 1
                    present.add(nid)
                except Exception as exc:
                    result.unrestorable.append({
                        "kind": "node", "node_type": ntype, "node_id": nid,
                        "source_artifact_ref": str(node["attrs"].get("source_artifact_ref") or ""),
                        "reason": f"node_create_failed:{type(exc).__name__}",
                    })
            for edge in snapshot.edges:
                from_id, to_id, etype = edge["from_id"], edge["to_id"], edge["edge_type"]
                from_node = node_by_id.get(from_id, {})
                from_ctx = {
                    "from_node_type": from_node.get("node_type", ""),
                    "from_source_artifact_ref": str(
                        (from_node.get("attrs") or {}).get("source_artifact_ref") or ""
                    ),
                }
                from_label = _node_label(conn, from_id)
                to_label = _node_label(conn, to_id)
                if from_label is None or to_label is None:
                    result.unrestorable.append({
                        "kind": "edge", "edge_type": etype, "from_id": from_id,
                        "to_id": to_id, "reason": "endpoint_absent_after_rebuild",
                        **from_ctx,
                    })
                    continue
                try:
                    orch.create_edge(
                        edge_type=etype, from_id=from_id, to_id=to_id,
                        attrs={}, from_type=from_label, to_type=to_label,
                    )
                    result.restored_edges += 1
                except Exception as exc:
                    result.unrestorable.append({
                        "kind": "edge", "edge_type": etype, "from_id": from_id,
                        "to_id": to_id,
                        "reason": f"edge_create_rejected:{type(exc).__name__}",
                        **from_ctx,
                    })
    except Exception as exc:  # mechanism itself broke -> integrity error upstream
        logger.warning(
            "kg.cognitive_preservation.restore_failed board=%s err=%s",
            board_id, exc,
        )
        result.error = f"restore_failed:{type(exc).__name__}"
    return result


def preservation_summary(
    snapshot: CognitiveSnapshot, restore: RestoreResult
) -> dict[str, Any]:
    """Structured, MCP/UI-readable preservation receipt for the rebuild report.

    ``status`` distinguishes clean vs degraded-with-trace vs unreadable vs
    integrity_error so a reader can tell a clean rebuild from a degraded one.
    """
    if restore.error is not None:
        status = STATUS_INTEGRITY_ERROR
    elif not snapshot.readable:
        status = STATUS_UNREADABLE
    elif restore.unrestorable:
        status = STATUS_DEGRADED
    else:
        status = STATUS_CLEAN
    return {
        "status": status,
        "readable": snapshot.readable,
        "snapshotted_nodes": snapshot.node_count,
        "snapshotted_edges": len(snapshot.edges),
        "restored_nodes": restore.restored_nodes,
        "restored_edges": restore.restored_edges,
        "unrestorable": restore.unrestorable,
        "unrestorable_count": len(restore.unrestorable),
        "error": restore.error,
    }


def record_cognitive_loss_fallback(
    board_id: str, summary: dict[str, Any], *, base_dir=None
) -> int:
    """Best-effort R7 traceability for unrestorable bug-derived Learnings: record
    a SYNC cognitive working-only hold (existing R7 readiness service — no parallel
    model). Returns the number of holds recorded. Non-bug-derived losses stay
    traceable via the report ``cognitive_preservation`` summary."""
    from okto_pulse.core.kg.canonical_learning_partition import _is_bug_derived_ref
    from okto_pulse.core.kg.connectivity_guard import (
        CANONICAL_LEARNING_WORKING_ONLY_REASON,
    )
    from okto_pulse.core.kg.rebuild_audit import record_cognitive_working_only_hold

    recorded = 0
    seen_refs: set[str] = set()
    for item in summary.get("unrestorable", []):
        # A lost bug-derived Learning NODE, or a lost validates edge FROM a
        # bug-derived Learning (the node survived but its canonical Bug evidence
        # could not be restored -> R7-incomplete) is the R7-applicable case.
        if item.get("kind") == "node" and item.get("node_type") == "Learning":
            source_ref = str(item.get("source_artifact_ref") or "")
        elif (
            item.get("kind") == "edge"
            and item.get("from_node_type") == "Learning"
        ):
            source_ref = str(item.get("from_source_artifact_ref") or "")
        else:
            continue
        if not source_ref or source_ref in seen_refs:
            continue
        if not _is_bug_derived_ref(source_ref):
            continue
        seen_refs.add(source_ref)
        try:
            res = record_cognitive_working_only_hold(
                board_id=board_id,
                hold_payload={
                    "reason_code": CANONICAL_LEARNING_WORKING_ONLY_REASON,
                    "source_ref": source_ref,
                    "artifact_type": "bug",
                    "observed_endpoints": [],
                    "session_id": "rebuild_cognitive_preservation",
                },
                actor_id="system:cognitive_preservation",
                base_dir=base_dir,
            )
            if res is not None:
                recorded += 1
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning(
                "kg.cognitive_preservation.fallback_hold_failed board=%s err=%s",
                board_id, exc,
            )
    return recorded


__all__ = [
    "COGNITIVE_TYPES",
    "STATUS_CLEAN",
    "STATUS_DEGRADED",
    "STATUS_UNREADABLE",
    "STATUS_INTEGRITY_ERROR",
    "CognitiveSnapshot",
    "RestoreResult",
    "snapshot_canonical_cognitive",
    "restore_canonical_cognitive",
    "preservation_summary",
    "record_cognitive_loss_fallback",
]
