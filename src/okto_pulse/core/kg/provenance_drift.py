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
* artifact row ``updated_at`` newer than the latest audit ``committed_at`` →
  ``content_changed`` (the artifact was edited and no re-consolidation has
  landed yet).

Both sides of the hash comparison use the SAME session recipe
(``compute_content_hash`` — persisted on the node at commit and on the
audit at every commit), never a new hash (TR6). The remedy is a normal
re-consolidation: the NC-8 restamp (FR3/D5) clears the flag. No writes —
the graph is untouched by this report.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.schema_contract import NODE_TYPES

logger = logging.getLogger("okto_pulse.kg.provenance_drift")

DRIFT_REPORT_MAX_ITEMS = 200
_NODE_FETCH_MAX_ROWS = 5000

__all__ = ["DRIFT_REPORT_MAX_ITEMS", "provenance_drift_report"]

_CARD_SOURCE_TYPES = frozenset({"task", "test", "bug"})


def _parse_dt(value: Any) -> datetime | None:
    """Best-effort ISO parse to an aware UTC datetime; None on failure."""

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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

    Nodes without a parseable ``type:id`` ref or whose artifact was never
    consolidated through the session path are counted as ``skipped`` —
    they carry no comparable anchor (technical roots, free-form refs).
    """

    if node_type is not None and node_type not in NODE_TYPES:
        raise ValueError(f"unknown node_type: {node_type}")
    registry = get_kg_registry()
    reader = registry.require_board_source_reader()
    rows = await asyncio.to_thread(reader.fetch, board_id)
    rows_by_ref = _index_source_rows(rows)

    types = [node_type] if node_type else list(NODE_TYPES)
    nodes = await asyncio.to_thread(_fetch_provenance_nodes, board_id, types)

    audit_cache: dict[str, Any] = {}
    drifted: list[dict[str, Any]] = []
    checked = 0
    skipped = 0

    for node in nodes:
        ref = node["source_artifact_ref"]
        if ":" not in ref:
            skipped += 1
            continue
        artifact_id = ref.split(":", 1)[1]
        if artifact_id not in audit_cache:
            audit_cache[artifact_id] = await registry.audit_repo.get_latest_for_artifact(
                board_id, artifact_id
            )
        audit = audit_cache[artifact_id]
        if audit is None:
            skipped += 1
            continue
        checked += 1

        entry = {
            "node_id": node["node_id"],
            "node_type": node["node_type"],
            "source_artifact_ref": ref,
            "persisted_hash": node["persisted_hash"],
            "current_hash": audit.content_hash,
        }
        row = rows_by_ref.get(ref)
        if row is None:
            entry["reason"] = "artifact_missing"
            drifted.append(entry)
            continue
        if node["persisted_hash"] != (audit.content_hash or ""):
            entry["reason"] = "content_changed"
            drifted.append(entry)
            continue
        row_updated = _parse_dt(row.get("updated_at"))
        audit_committed = _parse_dt(audit.committed_at)
        if (
            row_updated is not None
            and audit_committed is not None
            and row_updated > audit_committed
        ):
            entry["reason"] = "content_changed"
            entry["detail"] = "artifact_updated_after_last_consolidation"
            drifted.append(entry)

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
