"""Artifact→node provenance drift report (spec MKG-B-S1 — FR7, D5, TR6).

Read-only: compares the ``source_content_hash`` persisted on nodes (the
session-recipe hash of the assertion that last wrote their content — FR3)
against the artifact's consolidation history and current existence:

* node hash != latest audit content_hash for the same artifact →
  ``content_changed`` (the node's anchor is stale relative to the last
  consolidated state of the artifact — e.g. the fact is no longer attested,
  or the node is human_curated and its protected content diverged);
* artifact row absent from the BoardSourceReader → ``artifact_missing``
  (terminal drift — the source was deleted, D5);
* artifact row present but no consolidation audit exists → ``audit_missing``
  (the live source has no durable comparison anchor and is not healthy).
Artifact timestamps are deliberately excluded: they are volatile metadata and
cannot prove semantic drift. The committed canonical hash is the authority.

Both sides of the hash comparison use the SAME session recipe
(``compute_content_hash`` — persisted on the node at commit and on the
audit at every commit), never a new hash (TR6). The remedy is a normal
re-consolidation: the NC-8 restamp (FR3/D5) clears the flag. No writes —
the graph is untouched by this report.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.schema_contract import NODE_TYPES

logger = logging.getLogger("okto_pulse.kg.provenance_drift")

DRIFT_REPORT_MAX_ITEMS = 200
_NODE_FETCH_MAX_ROWS = 5000

__all__ = ["DRIFT_REPORT_MAX_ITEMS", "provenance_drift_report"]

_CARD_SOURCE_TYPES = frozenset({"task", "test", "bug"})


def _fetch_provenance_nodes(
    board_id: str, node_types: list[str]
) -> list[dict[str, Any]]:
    """Sync read of active nodes that carry a provenance anchor."""

    executor = get_kg_registry().cypher_executor
    if executor is None or not executor.is_supported():
        raise RuntimeError(
            "provenance drift requires a composed cypher_executor"
        )
    nodes: list[dict[str, Any]] = []
    for node_type in node_types:
        result = executor.execute_read_only(
            board_id,
            f"MATCH (n:{node_type}) "
            f"WHERE n.source_content_hash IS NOT NULL "
            f"AND n.superseded_by IS NULL "
            f"RETURN n.id, n.source_artifact_ref, n.source_content_hash",
            {},
            max_rows=_NODE_FETCH_MAX_ROWS,
        )
        for row in result.get("rows", []):
            nodes.append(
                {
                    "node_id": row[0],
                    "node_type": node_type,
                    "source_artifact_ref": str(row[1] or ""),
                    "persisted_hash": str(row[2] or ""),
                }
            )
    return nodes


def _index_source_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index source rows under every equivalent persisted reference.

    The board source port exposes cards by their semantic type (``task``,
    ``test`` or ``bug``), while graph entities created by the generic card
    path can carry ``card:<id>``. Both references identify the same source
    row and must not produce terminal ``artifact_missing`` drift.
    """

    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        source_ref = str(row.get("source_ref") or "")
        if source_ref:
            indexed[source_ref] = row
        artifact_type = str(row.get("artifact_type") or "").lower()
        artifact_id = str(row.get("id") or "")
        if artifact_type in _CARD_SOURCE_TYPES and artifact_id:
            indexed[f"card:{artifact_id}"] = row
    return indexed


async def provenance_drift_report(
    board_id: str,
    node_type: str | None = None,
    *,
    max_items: int = DRIFT_REPORT_MAX_ITEMS,
) -> dict[str, Any]:
    """Build the drift report for one board (optionally one node type).

    Only nodes without a parseable ``type:id`` ref are counted as ``skipped``
    (technical roots and free-form refs). A parseable node is always evaluated:
    a missing source is ``artifact_missing`` even when its audit is also absent,
    while a live source without an audit is ``audit_missing``.
    """

    if node_type is not None and node_type not in NODE_TYPES:
        raise ValueError(f"unknown node_type: {node_type}")
    registry = get_kg_registry()
    reader = registry.require_board_source_reader()
    snapshot = await asyncio.to_thread(reader.fetch, board_id)
    if not snapshot.complete:
        from okto_pulse.core.kg.interfaces import SourceUnavailableError

        raise SourceUnavailableError(
            "provenance drift source snapshot is incomplete "
            f"(board_id={board_id}, cause={snapshot.cause})",
            cause_type=str(snapshot.cause or "unknown"),
        )
    rows_by_ref = _index_source_rows(snapshot.rows)

    types = [node_type] if node_type else list(NODE_TYPES)
    nodes = await asyncio.to_thread(_fetch_provenance_nodes, board_id, types)

    # Artifact ids are not globally unique across source types. Keep the same
    # complete identity in the local cache that the typed repository lookup
    # resolves.
    audit_cache: dict[tuple[str, str], Any] = {}
    drifted: list[dict[str, Any]] = []
    checked = 0
    skipped = 0

    for node in nodes:
        ref = node["source_artifact_ref"]
        artifact_type, separator, artifact_id = ref.partition(":")
        if not separator or not artifact_type or not artifact_id:
            skipped += 1
            continue
        artifact_type = artifact_type.lower()
        checked += 1
        audit_identity = (artifact_type, artifact_id)
        if audit_identity not in audit_cache:
            audit = await registry.audit_repo.get_latest_for_artifact(
                board_id,
                artifact_id,
                artifact_type=artifact_type,
            )
            audit_type = str(
                getattr(audit, "artifact_type", "") or ""
            ).lower()
            # Defense in depth for a misbehaving custom provider: a cross-type
            # audit is not an anchor for this node.
            if audit is not None and audit_type and audit_type != artifact_type:
                audit = None
            audit_cache[audit_identity] = audit
        audit = audit_cache[audit_identity]

        entry = {
            "node_id": node["node_id"],
            "node_type": node["node_type"],
            "source_artifact_ref": ref,
            "persisted_hash": node["persisted_hash"],
            "current_hash": (
                audit.content_hash if audit is not None else None
            ),
        }
        row = rows_by_ref.get(ref)
        if row is None:
            entry["reason"] = "artifact_missing"
            drifted.append(entry)
            continue
        if audit is None:
            entry["reason"] = "audit_missing"
            drifted.append(entry)
            continue
        if node["persisted_hash"] != (audit.content_hash or ""):
            entry["reason"] = "content_changed"
            drifted.append(entry)
            continue

    truncated = len(drifted) > max_items
    report = {
        "board_id": board_id,
        "node_type": node_type,
        # OR2 — checked_count/drifted_count with per-reason split.
        "checked_count": checked,
        "skipped_count": skipped,
        "drifted_count": len(drifted),
        "drifted_by_reason": {
            "content_changed": sum(
                1 for d in drifted if d["reason"] == "content_changed"
            ),
            "artifact_missing": sum(
                1 for d in drifted if d["reason"] == "artifact_missing"
            ),
            "audit_missing": sum(
                1 for d in drifted if d["reason"] == "audit_missing"
            ),
        },
        "drifted": drifted[:max_items],
        "truncated": truncated,
    }
    logger.info(
        "kg.provenance.drift_report board=%s checked=%d drifted=%d skipped=%d",
        board_id, checked, len(drifted), skipped,
        extra={
            "event": "kg.provenance.drift_report",
            "board_id": board_id,
            "checked_count": checked,
            "drifted_count": len(drifted),
            "skipped_count": skipped,
        },
    )
    return report
