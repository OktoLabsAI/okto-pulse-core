"""NC-8 — one-shot migration to consolidate duplicate Kuzu nodes.

Spec 7f23535f. Identifies groups `(node_type, source_artifact_ref)` with
count > 1 in the per-board Kuzu graph, picks the most-recent node as
canonical, re-points all incoming/outgoing edges of duplicates to the
canonical, then `DETACH DELETE`s the duplicates.

Idempotent — running on a clean board reports 0 actions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg.schema import (
    EDGE_METADATA_COLUMNS,
    MULTI_REL_TYPES,
    NODE_TYPES,
    REL_TYPES,
    open_board_connection,
)

logger = logging.getLogger("okto_pulse.kg.dedup_migration")

# Node attrs we read for tie-break / canonical pick. Must align with the
# schema in core/kg/schema.py — only `created_at` is required for the
# "most recent wins" rule. Others surface in the report for ops triage.
_NODE_REPORT_COLS = ("id", "created_at", "title", "human_curated")


def _all_rel_pairs() -> list[tuple[str, str, str]]:
    """Flatten REL_TYPES + MULTI_REL_TYPES into a single list of triples
    `(rel_name, from_type, to_type)` so the migration can iterate over
    every edge variant without case-by-case logic.
    """
    out: list[tuple[str, str, str]] = list(REL_TYPES)
    for rel_name, pairs in MULTI_REL_TYPES:
        for from_t, to_t in pairs:
            out.append((rel_name, from_t, to_t))
    return out


def _fetch_groups(kconn, node_type: str) -> list[dict[str, Any]]:
    """Return list of duplicate groups for a node type.

    Each group: `{source_artifact_ref, count, members: [{id, created_at,
    title, human_curated}]}`. Empty list when no duplicates exist —
    callers can short-circuit.
    """
    res = kconn.execute(
        f"MATCH (n:{node_type}) "
        f"WHERE n.source_artifact_ref <> '' "
        f"RETURN n.source_artifact_ref, n.id, n.created_at, n.title, "
        f"n.human_curated"
    )
    rows: dict[str, list[dict[str, Any]]] = {}
    try:
        while res.has_next():
            row = res.get_next()
            ref = row[0]
            rows.setdefault(ref, []).append({
                "id": row[1],
                "created_at": row[2],
                "title": row[3],
                "human_curated": row[4],
            })
    finally:
        try:
            res.close()
        except Exception:
            pass
    groups: list[dict[str, Any]] = []
    for ref, members in rows.items():
        if len(members) <= 1:
            continue
        members.sort(key=lambda m: m["created_at"] or "", reverse=True)
        groups.append({
            "source_artifact_ref": ref,
            "count": len(members),
            "members": members,
        })
    return groups


def _repoint_edges(
    kconn,
    rel_name: str,
    from_type: str,
    to_type: str,
    duplicate_id: str,
    canonical_id: str,
) -> int:
    """Re-point every `(:from_type)-[r:rel_name]->(:to_type)` edge that
    touches `duplicate_id` to use `canonical_id` instead. Returns the
    number of edges re-pointed (0 if none exist for this rel pair).

    Strategy: read all matching edges with their attrs, DELETE old, then
    CREATE new with the same attrs against the canonical id. Kuzu has no
    primitive REL UPDATE that swaps endpoints in place.
    """
    attr_cols = ", ".join(f"r.{name}" for name, _ in EDGE_METADATA_COLUMNS)
    # Outbound edges (duplicate is FROM endpoint)
    out_count = _repoint_outbound(
        kconn, rel_name, from_type, to_type, duplicate_id, canonical_id, attr_cols
    )
    # Inbound edges (duplicate is TO endpoint)
    in_count = _repoint_inbound(
        kconn, rel_name, from_type, to_type, duplicate_id, canonical_id, attr_cols
    )
    return out_count + in_count


def _read_edge_rows(kconn, cypher: str, params: dict) -> list[tuple]:
    res = kconn.execute(cypher, params)
    out: list[tuple] = []
    try:
        while res.has_next():
            out.append(tuple(res.get_next()))
    finally:
        try:
            res.close()
        except Exception:
            pass
    return out


def _repoint_outbound(
    kconn, rel_name, from_type, to_type, dup_id, canonical_id, attr_cols
) -> int:
    cypher_read = (
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE a.id = $dup "
        f"RETURN b.id, r.confidence, {attr_cols}"
    )
    rows = _read_edge_rows(kconn, cypher_read, {"dup": dup_id})
    if not rows:
        return 0
    # DELETE first to avoid Kuzu uniqueness when re-creating same edge
    kconn.execute(
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE a.id = $dup DELETE r",
        {"dup": dup_id},
    )
    for row in rows:
        target_id, confidence = row[0], row[1]
        layer, rule_id, created_by, fallback_reason = row[2], row[3], row[4], row[5]
        params = {
            "src": canonical_id,
            "tgt": target_id,
            "conf": confidence,
            "layer": layer,
            "rule_id": rule_id,
            "created_by": created_by,
            "fallback_reason": fallback_reason,
        }
        kconn.execute(
            f"MATCH (a:{from_type}) WHERE a.id = $src "
            f"MATCH (b:{to_type}) WHERE b.id = $tgt "
            f"CREATE (a)-[:{rel_name} {{confidence: $conf, layer: $layer, "
            f"rule_id: $rule_id, created_by: $created_by, "
            f"fallback_reason: $fallback_reason}}]->(b)",
            params,
        )
    return len(rows)


def _repoint_inbound(
    kconn, rel_name, from_type, to_type, dup_id, canonical_id, attr_cols
) -> int:
    cypher_read = (
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE b.id = $dup "
        f"RETURN a.id, r.confidence, {attr_cols}"
    )
    rows = _read_edge_rows(kconn, cypher_read, {"dup": dup_id})
    if not rows:
        return 0
    kconn.execute(
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE b.id = $dup DELETE r",
        {"dup": dup_id},
    )
    for row in rows:
        source_id, confidence = row[0], row[1]
        layer, rule_id, created_by, fallback_reason = row[2], row[3], row[4], row[5]
        params = {
            "src": source_id,
            "tgt": canonical_id,
            "conf": confidence,
            "layer": layer,
            "rule_id": rule_id,
            "created_by": created_by,
            "fallback_reason": fallback_reason,
        }
        kconn.execute(
            f"MATCH (a:{from_type}) WHERE a.id = $src "
            f"MATCH (b:{to_type}) WHERE b.id = $tgt "
            f"CREATE (a)-[:{rel_name} {{confidence: $conf, layer: $layer, "
            f"rule_id: $rule_id, created_by: $created_by, "
            f"fallback_reason: $fallback_reason}}]->(b)",
            params,
        )
    return len(rows)


def _delete_node(kconn, node_type: str, node_id: str) -> None:
    kconn.execute(
        f"MATCH (n:{node_type}) WHERE n.id = $id DETACH DELETE n",
        {"id": node_id},
    )


def migrate_dedup_entities(
    board_id: str,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run dedup migration for a single board, return structured report.

    On dry_run=True, all read steps execute (lookup duplicates, simulate
    edge counts) but no DELETE/CREATE writes happen. Idempotent: a board
    with zero duplicates returns `{groups: 0, total_duplicates_removed:
    0, edges_repointed: 0}` immediately.
    """
    started = datetime.now(timezone.utc).isoformat()
    rel_pairs = _all_rel_pairs()
    groups_summary: list[dict[str, Any]] = []
    total_dups = 0
    total_edges = 0

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        for node_type in NODE_TYPES:
            try:
                groups = _fetch_groups(kconn, node_type)
            except Exception as exc:
                logger.warning(
                    "kg.dedup.scan_failed type=%s board=%s err=%s",
                    node_type, board_id, exc,
                    extra={
                        "event": "kg.dedup.scan_failed",
                        "node_type": node_type,
                        "board_id": board_id,
                    },
                )
                continue
            for group in groups:
                members = group["members"]
                canonical = members[0]
                duplicates = members[1:]
                edges_for_group = 0
                for dup in duplicates:
                    if not dry_run:
                        for rel_name, from_t, to_t in rel_pairs:
                            try:
                                edges_for_group += _repoint_edges(
                                    kconn,
                                    rel_name, from_t, to_t,
                                    dup["id"], canonical["id"],
                                )
                            except Exception as exc:
                                # Edge re-point failure is non-fatal —
                                # log and continue. Operator sees this
                                # in the report for triage.
                                logger.warning(
                                    "kg.dedup.repoint_failed rel=%s "
                                    "from=%s to=%s dup=%s canonical=%s "
                                    "err=%s",
                                    rel_name, from_t, to_t,
                                    dup["id"], canonical["id"], exc,
                                    extra={
                                        "event": "kg.dedup.repoint_failed",
                                    },
                                )
                        _delete_node(kconn, node_type, dup["id"])
                    else:
                        # Dry-run: count the edges that WOULD be moved.
                        for rel_name, from_t, to_t in rel_pairs:
                            try:
                                # Simpler count via direct match:
                                res = kconn.execute(
                                    f"MATCH (a:{from_t})-[r:{rel_name}]->"
                                    f"(b:{to_t}) "
                                    f"WHERE a.id = $dup OR b.id = $dup "
                                    f"RETURN count(r)",
                                    {"dup": dup["id"]},
                                )
                                try:
                                    row = res.get_next()
                                    edges_for_group += int(row[0])
                                finally:
                                    try:
                                        res.close()
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                groups_summary.append({
                    "node_type": node_type,
                    "source_artifact_ref": group["source_artifact_ref"],
                    "duplicates_found": group["count"],
                    "canonical_id": canonical["id"],
                    "edges_repointed": edges_for_group,
                    "deleted_ids": [d["id"] for d in duplicates],
                })
                total_dups += len(duplicates)
                total_edges += edges_for_group
    completed = datetime.now(timezone.utc).isoformat()
    report = {
        "board_id": board_id,
        "dry_run": dry_run,
        "groups": len(groups_summary),
        "total_duplicates_removed": 0 if dry_run else total_dups,
        "duplicates_planned": total_dups if dry_run else total_dups,
        "edges_repointed": 0 if dry_run else total_edges,
        "edges_planned": total_edges if dry_run else total_edges,
        "started_at": started,
        "executed_at": completed,
        "details": groups_summary,
    }
    logger.info(
        "kg.dedup.completed board=%s dry_run=%s groups=%d dups=%d "
        "edges=%d",
        board_id, dry_run, report["groups"], total_dups, total_edges,
        extra={
            "event": "kg.dedup.completed",
            "board_id": board_id,
            "dry_run": dry_run,
            "groups": report["groups"],
            "duplicates_planned": total_dups,
            "edges_planned": total_edges,
        },
    )
    return report


def format_report_table(report: dict[str, Any]) -> str:
    """Render a human-readable table from the structured report.

    Fixed-width columns sized for ops triage in a terminal at 120 cols.
    Truncates source_artifact_ref and canonical_id to keep rows on one
    line; the JSON output (--json flag) carries the full IDs.
    """
    header = (
        f"{'node_type':<14} | {'source_artifact_ref':<40} | "
        f"{'duplicates_found':>16} | {'canonical_id':<32} | "
        f"{'edges_repointed':>15}"
    )
    separator = "-" * len(header)
    lines = [
        f"KG dedup migration — board {report['board_id']} "
        f"{'(DRY-RUN)' if report['dry_run'] else '(APPLIED)'}",
        header,
        separator,
    ]
    for entry in report["details"]:
        ref = entry["source_artifact_ref"]
        if len(ref) > 40:
            ref = ref[:37] + "..."
        canonical = entry["canonical_id"]
        if len(canonical) > 32:
            canonical = canonical[:29] + "..."
        lines.append(
            f"{entry['node_type']:<14} | {ref:<40} | "
            f"{entry['duplicates_found']:>16} | {canonical:<32} | "
            f"{entry['edges_repointed']:>15}"
        )
    lines.append(separator)
    summary_label = "Planned" if report["dry_run"] else "Applied"
    lines.append(
        f"{summary_label}: groups={report['groups']} "
        f"duplicates={report.get('duplicates_planned', report.get('total_duplicates_removed', 0))} "
        f"edges={report.get('edges_planned', report.get('edges_repointed', 0))}"
    )
    return "\n".join(lines)
