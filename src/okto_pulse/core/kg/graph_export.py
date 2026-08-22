"""Logical graph export — JSON-LD with a fixed PROV-O mapping
(spec MKG-E-S1 — FR5, TR5/TR6, D4/D5, BR4/BR5).

First engine-independent serialization of a board graph. Strictly
READ-ONLY through the composed ``cypher_executor`` port (never opens the
storage directly — D5/R7) and deterministic by construction: nodes are
ordered by (node_type, node_id), edges by (rel_type, from, to), and the
callers serialize with ``sort_keys`` — the same board always produces the
same bytes (a diffable logical backup, the KGD-01 lesson).

Vocabulary (D4 — the whole @context):
  * node        → ``prov:Entity`` + ``pulse:nodeType`` (+ ``pulse:kindOf``)
  * source_artifact_ref  → ``prov:wasDerivedFrom``
  * source_session_id    → ``prov:wasGeneratedBy``
  * created_by_agent     → ``prov:wasAttributedTo``
  * supersedence         → ``prov:wasRevisionOf`` on the successor
                           (+ ``pulse:supersededBy`` on the predecessor)
  * every edge           → ``pulse:Edge`` with ``pulse:relType``

An unreadable graph raises :class:`GraphExportError` (stable code
``kg_export_failed``) with NO partial output.
"""

from __future__ import annotations

import logging
from typing import Any

from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.cypher_templates import (
    code_traceability_visibility_clause,
)
from okto_pulse.core.kg.schema_contract import (
    MULTI_REL_TYPES,
    NODE_TYPES,
    REL_TYPES,
)

logger = logging.getLogger("okto_pulse.kg.graph_export")

JSONLD_CONTEXT = {
    "pulse": "https://oktolabs.ai/pulse/kg#",
    "prov": "http://www.w3.org/ns/prov#",
}

_NODE_FETCH_MAX_ROWS = 100_000
_EDGE_FETCH_MAX_ROWS = 100_000

__all__ = ["GraphExportError", "JSONLD_CONTEXT", "export_board_jsonld"]


class GraphExportError(Exception):
    """Stable code ``kg_export_failed`` — the export produced NO output."""

    code = "kg_export_failed"

    def __init__(self, board_id: str, reason: str) -> None:
        self.board_id = board_id
        self.reason = reason
        super().__init__(f"kg_export_failed: board={board_id} reason={reason}")


def _executor():
    executor = get_kg_registry().cypher_executor
    if executor is None or not executor.is_supported():
        raise RuntimeError("graph export requires a composed cypher_executor")
    return executor


def _all_rel_pairs() -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = list(REL_TYPES)
    for rel_name, pairs in MULTI_REL_TYPES:
        for from_t, to_t in pairs:
            out.append((rel_name, from_t, to_t))
    return sorted(out)


def _fetch_nodes(
    board_id: str,
    *,
    include_code_traceability: bool,
) -> list[dict[str, Any]]:
    executor = _executor()
    nodes: list[dict[str, Any]] = []
    for node_type in sorted(NODE_TYPES):
        result = executor.execute_read_only(
            board_id,
            f"MATCH (n:{node_type}) "
            f"WHERE {code_traceability_visibility_clause('n')} "
            f"RETURN n.id, n.title, n.content, n.created_at, "
            f"n.source_artifact_ref, n.source_session_id, "
            f"n.created_by_agent, n.superseded_by, n.kind_of "
            f"ORDER BY n.id",
            {"include_code_traceability": include_code_traceability},
            max_rows=_NODE_FETCH_MAX_ROWS,
        )
        for row in result.get("rows", []):
            nodes.append(
                {
                    "node_type": node_type,
                    "node_id": row[0],
                    "title": row[1],
                    "content": row[2],
                    "created_at": row[3],
                    "source_artifact_ref": row[4],
                    "source_session_id": row[5],
                    "created_by_agent": row[6],
                    "superseded_by": row[7],
                    "kind_of": row[8],
                }
            )
    return nodes


def _fetch_edges(
    board_id: str,
    node_ids: set[str] | None,
    *,
    include_code_traceability: bool,
) -> list[dict[str, Any]]:
    executor = _executor()
    edges: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    for rel_name, from_t, to_t in _all_rel_pairs():
        result = executor.execute_read_only(
            board_id,
            f"MATCH (a:{from_t})-[r:{rel_name}]->(b:{to_t}) "
            f"WHERE {code_traceability_visibility_clause('a')} "
            f"AND {code_traceability_visibility_clause('b')} "
            f"RETURN a.id, b.id, r.confidence ORDER BY a.id, b.id",
            {"include_code_traceability": include_code_traceability},
            max_rows=_EDGE_FETCH_MAX_ROWS,
        )
        for row in result.get("rows", []):
            if node_ids is not None and row[0] not in node_ids:
                # Paged export: an edge travels with the page of its FROM
                # endpoint so page concatenation reconstructs the full set.
                continue
            key = (rel_name, row[0], row[1])
            if key in seen:
                continue
            seen.add(key)
            edges.append(
                {
                    "rel_type": rel_name,
                    "from": row[0],
                    "to": row[1],
                    "confidence": row[2],
                }
            )
    edges.sort(key=lambda e: (e["rel_type"], e["from"], e["to"]))
    return edges


def _node_to_jsonld(
    node: dict[str, Any], revision_of: dict[str, str]
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "@id": f"pulse:{node['node_id']}",
        "@type": "prov:Entity",
        "pulse:nodeType": node["node_type"],
    }
    if node.get("kind_of"):
        entry["pulse:kindOf"] = node["kind_of"]
    if node.get("title") is not None:
        entry["pulse:title"] = node["title"]
    if node.get("content"):
        entry["pulse:content"] = node["content"]
    if node.get("created_at") is not None:
        entry["pulse:createdAt"] = str(node["created_at"])
    if node.get("source_artifact_ref"):
        entry["prov:wasDerivedFrom"] = node["source_artifact_ref"]
    if node.get("source_session_id"):
        entry["prov:wasGeneratedBy"] = f"pulse:session/{node['source_session_id']}"
    if node.get("created_by_agent"):
        entry["prov:wasAttributedTo"] = f"pulse:agent/{node['created_by_agent']}"
    if node.get("superseded_by"):
        entry["pulse:supersededBy"] = f"pulse:{node['superseded_by']}"
    predecessor = revision_of.get(node["node_id"])
    if predecessor:
        entry["prov:wasRevisionOf"] = f"pulse:{predecessor}"
    return entry


def _edge_to_jsonld(edge: dict[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "@type": "pulse:Edge",
        "pulse:relType": edge["rel_type"],
        "pulse:from": f"pulse:{edge['from']}",
        "pulse:to": f"pulse:{edge['to']}",
    }
    if edge.get("confidence") is not None:
        entry["pulse:confidence"] = edge["confidence"]
    return entry


def export_board_jsonld(
    board_id: str,
    *,
    cursor: str | None = None,
    page_size: int | None = None,
    include_code_traceability: bool = True,
) -> dict[str, Any]:
    """Serialize a board graph to JSON-LD (read-only, deterministic).

    Full export (``page_size=None``) returns every node and edge. Paged
    export (MCP surface — FR6) orders nodes globally by (node_type,
    node_id), starts strictly AFTER ``cursor`` (a node_id) and carries the
    edges whose FROM endpoint is inside the page; the response exposes
    ``next_cursor``/``last_page`` so the concatenation of pages is the
    full export.
    """

    try:
        all_nodes = _fetch_nodes(
            board_id,
            include_code_traceability=include_code_traceability,
        )
        all_nodes.sort(key=lambda n: (n["node_type"], n["node_id"]))

        page_nodes = all_nodes
        next_cursor: str | None = None
        last_page = True
        if page_size is not None:
            size = max(1, int(page_size))
            start = 0
            if cursor:
                for i, node in enumerate(all_nodes):
                    if node["node_id"] == cursor:
                        start = i + 1
                        break
            page_nodes = all_nodes[start : start + size]
            if start + size < len(all_nodes):
                last_page = False
                next_cursor = page_nodes[-1]["node_id"] if page_nodes else None

        node_ids = (
            None
            if page_size is None
            else {n["node_id"] for n in page_nodes}
        )
        edges = _fetch_edges(
            board_id,
            node_ids,
            include_code_traceability=include_code_traceability,
        )

        # prov:wasRevisionOf lives on the SUCCESSOR — invert superseded_by.
        revision_of: dict[str, str] = {}
        for node in all_nodes:
            if node.get("superseded_by"):
                revision_of[node["superseded_by"]] = node["node_id"]

        graph: list[dict[str, Any]] = [
            _node_to_jsonld(n, revision_of) for n in page_nodes
        ]
        graph.extend(_edge_to_jsonld(e) for e in edges)

        document: dict[str, Any] = {
            "@context": dict(JSONLD_CONTEXT),
            "pulse:board": board_id,
            "@graph": graph,
            "nodes_exported": len(page_nodes),
            "edges_exported": len(edges),
        }
        if page_size is not None:
            document["next_cursor"] = next_cursor
            document["last_page"] = last_page
        logger.info(
            "kg.export.completed board=%s nodes=%d edges=%d paged=%s",
            board_id, len(page_nodes), len(edges), page_size is not None,
            extra={
                "event": "kg.export.completed",
                "board_id": board_id,
                "nodes_exported": len(page_nodes),
                "edges_exported": len(edges),
            },
        )
        return document
    except GraphExportError:
        raise
    except Exception as exc:
        logger.error(
            "kg.export.failed board=%s err=%s", board_id, exc,
            extra={
                "event": "kg.export.failed",
                "board_id": board_id,
                "reason": str(exc),
            },
        )
        raise GraphExportError(board_id, str(exc)) from exc
