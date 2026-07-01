"""REST API endpoints under /api/kg/ for the Knowledge Graph dashboard.

Thin adapters over `kg_service` + `tier_power`. All endpoints share:
- Auth via existing get_current_user dependency
- RFC 7807 Problem Details for errors
- ETag caching via W/"hash(last_consolidation_timestamp)"
- Cursor-based pagination (base64 JSON cursor)
- CORS configured via env OKTO_PULSE_CORS_ALLOWED_ORIGINS
- Rate limiting per endpoint category
"""

from __future__ import annotations

import base64
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from okto_pulse.core.kg.kg_service import (
    KGToolError,
    get_kg_service,
    normalize_graph_layer,
)
from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    execute_cypher_read_only,
    get_schema_info,
)
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.inbound.rest_adapter import RESTAdapterContract
from okto_pulse.core.repositories import PulseUnitOfWork
from okto_pulse.core.application.use_cases.kg_routes_crud import (
    BoostNodeCommand,
    BoostNodeUseCase,
    CancelHistoricalCommand,
    CancelHistoricalUseCase,
    DeleteBoardKgCommand,
    DeleteBoardKgUseCase,
    GetHistoricalProgressCommand,
    GetHistoricalProgressUseCase,
    GlobalSearchCommand,
    GlobalSearchUseCase,
    ListAuditCommand,
    ListAuditUseCase,
    ListPendingCommand,
    ListPendingTreeCommand,
    ListPendingTreeUseCase,
    ListPendingUseCase,
    RetryPendingEntryCommand,
    RetryPendingEntryUseCase,
    StartHistoricalCommand,
    StartHistoricalUseCase,
)

router = APIRouter(prefix="/kg", tags=["knowledge-graph"])

# These KG dashboard endpoints are unauthenticated (community/local). The actor is
# a formality for the transport-free use cases — the governance/reader delegates do
# not key off it — so a fixed local-user identity is passed, matching the
# ``local-user`` convention already used by ``boost_node`` in this module.
_KG_ACTOR_ID = "local-user"
logger = logging.getLogger("okto_pulse.api.kg_routes")


# ---------------------------------------------------------------------------
# RFC 7807 Problem Details
# ---------------------------------------------------------------------------


class ProblemDetail(BaseModel):
    type: str = "/errors/internal"
    title: str = "Internal Server Error"
    status: int = 500
    detail: str = ""
    instance: str = ""


def _problem(status: int, title: str, detail: str, error_type: str = "") -> JSONResponse:
    body = ProblemDetail(
        type=f"/errors/{error_type or title.lower().replace(' ', '-')}",
        title=title,
        status=status,
        detail=detail,
    )
    return JSONResponse(
        status_code=status,
        content=body.model_dump(),
        media_type="application/problem+json",
    )


def _handle_kg_error(e: KGToolError) -> JSONResponse:
    code_map = {
        "not_found": 404,
        "permission_denied": 403,
        "invalid_param": 400,
        "kuzu_error": 500,
        "schema_drift": 409,
        "empty_result": 404,
    }
    status = code_map.get(e.code, 500)
    return _problem(status, e.code, e.message, e.code)


# ---------------------------------------------------------------------------
# ETag helpers
# ---------------------------------------------------------------------------


def _compute_etag(board_id: str) -> str:
    ts = datetime.now(timezone.utc).isoformat()[:16]
    raw = f"{board_id}:{ts}"
    return f'W/"{hashlib.md5(raw.encode()).hexdigest()[:16]}"'


# ---------------------------------------------------------------------------
# Cursor pagination helpers
# ---------------------------------------------------------------------------


def encode_cursor(created_at_iso: str, node_id: str) -> str:
    """Encode a (created_at, id) tuple as an opaque base64 cursor.

    Format: ``base64("<iso_timestamp>;<node_id>")`` — a semicolon separator
    keeps the codec trivial and survives round-tripping through the query
    string. Spec 8 / S1.2.
    """
    if not created_at_iso or not node_id:
        raise ValueError("cursor components must be non-empty")
    payload = f"{created_at_iso};{node_id}"
    return base64.b64encode(payload.encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, str]:
    """Decode a cursor produced by :func:`encode_cursor`.

    Raises ``ValueError`` on any corruption — the route handler translates
    that into HTTP 410 Gone per AC-12 / S1.5. Returning a sentinel would
    hide a corrupted cursor behind "first page" which is worse UX.
    """
    try:
        payload = base64.b64decode(cursor.encode()).decode()
    except Exception as exc:
        raise ValueError(f"cursor is not valid base64: {exc}") from exc
    if ";" not in payload:
        raise ValueError("cursor missing separator")
    created_at, _, node_id = payload.partition(";")
    if not created_at or not node_id:
        raise ValueError("cursor has empty components")
    return created_at, node_id


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/nodes")
async def list_nodes(
    board_id: str,
    type: str = "",
    min_confidence: float = 0.5,
    min_relevance: float = Query(0.0, ge=0.0),
    limit: int = Query(50, ge=1, le=200),
    cursor: str = "",
    graph_layer: str = "canonical",
):
    """List KG nodes with filters and cursor pagination."""
    svc = get_kg_service()
    try:
        layer = normalize_graph_layer(graph_layer)
        rows = svc.get_all_nodes(
            board_id,
            min_confidence=min_confidence,
            min_relevance=min_relevance,
            max_rows=limit,
            cursor=cursor or None,
            node_type=type or None,
            graph_layer=layer,
        )
        total_hint = svc.count_all_nodes(
            board_id,
            min_confidence=min_confidence,
            min_relevance=min_relevance,
            node_type=type or None,
            graph_layer=layer,
        )
        return {
            "nodes": rows,
            "next_cursor": _next_cursor_for(rows, limit),
            "total_hint": total_hint,
            "page_count": len(rows),
            "graph_layer": layer,
        }
    except ValueError as exc:
        return _problem(
            410,
            "Gone",
            f"cursor is invalid or corrupted: {exc}",
            "invalid_cursor",
        )
    except KGToolError as e:
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/nodes/{node_id}")
async def get_node_detail(board_id: str, node_id: str):
    """Get node detail across any node type in the board graph."""
    svc = get_kg_service()
    try:
        result = svc.get_node_detail(board_id, node_id)
        if result is None:
            return _problem(404, "Not Found", f"Node {node_id} not found")
        return result
    except KGToolError as e:
        if e.code == "not_found":
            return _problem(404, "Not Found", f"Node {node_id} not found")
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/graph")
async def get_subgraph(
    board_id: str,
    center: str = "",
    depth: int = Query(2, ge=1, le=5),
    limit: int = Query(100),
    cursor: str = "",
    min_relevance: float = Query(0.0, ge=0.0),
    type: str = "",
    graph_layer: str = "canonical",
):
    """Return subgraph for visualization — Spec 8 / S1.1, S1.4, S1.5.

    Pagination contract:

    * ``limit`` in [1, 1000]; out-of-range values yield **400** (not 422) per
      AC-11 so clients get a human-readable reason instead of Pydantic's
      validation schema.
    * ``cursor`` is an opaque base64 token emitted by a prior call. A
      corrupted cursor yields **410 Gone** per AC-12 — it cannot be
      silently reinterpreted as "first page".
    * Response always carries ``next_cursor``. It is ``None`` when the
      returned page is the last one so clients can stop paging without a
      second round trip.
    """
    if limit < 1 or limit > 1000:
        return _problem(
            400,
            "Bad Request",
            f"limit must be in range [1, 1000], got {limit}",
            "invalid_limit",
        )

    svc = get_kg_service()
    try:
        layer = normalize_graph_layer(graph_layer)
        if center:
            # Spec 849d6292 (FR6/AC5): the centered branch MUST scope to the
            # requested layer too — default canonical never leaks working.
            rows = svc.get_related_context(
                board_id, center, max_rows=limit, graph_layer=layer,
            )
            next_cursor: str | None = None
        else:
            try:
                rows = svc.get_all_nodes(
                    board_id,
                    min_confidence=0.0,
                    min_relevance=min_relevance,
                    max_rows=limit,
                    cursor=cursor or None,
                    node_type=type or None,
                    graph_layer=layer,
                )
            except ValueError as exc:
                return _problem(
                    410,
                    "Gone",
                    f"cursor is invalid or corrupted: {exc}",
                    "invalid_cursor",
                )
            next_cursor = _next_cursor_for(rows, limit)

        node_ids = {_node_id(r) for r in rows if _node_id(r)}
        edges, edge_metadata = _fetch_edges_for_nodes(board_id, node_ids)

        return {
            "nodes": rows,
            "edges": edges,
            "next_cursor": next_cursor,
            "metadata": {
                "depth": depth,
                "truncated": len(rows) >= limit,
                "min_relevance": min_relevance,
                "graph_layer": layer,
                **edge_metadata,
            },
        }
    except KGToolError as e:
        return _handle_kg_error(e)


def _node_id(row: Any) -> str:
    if isinstance(row, dict):
        return str(row.get("id") or "")
    if isinstance(row, (list, tuple)) and row:
        return str(row[0] or "")
    return ""


def _next_cursor_for(rows: list[dict], limit: int) -> str | None:
    """Return a cursor pointing past the last row, or ``None`` on last page.

    A short page (fewer rows than requested) is unambiguously the last one,
    so we omit the cursor and clients stop paging. A full page may or may
    not have more — we always emit a cursor because a follow-up call is
    cheap and returning an empty page is a valid terminator.
    """
    if not rows or len(rows) < limit:
        return None
    last = rows[-1]
    created_at = last.get("created_at")
    node_id = last.get("id")
    if not created_at or not node_id:
        return None
    return encode_cursor(str(created_at), str(node_id))


def _fetch_edges_for_nodes(
    board_id: str, node_ids: set[str],
) -> tuple[list[dict], dict[str, Any]]:
    """Fetch all edges between the given node IDs from the board graph.

    Edge reads are diagnostic: relationship-table failures are returned in
    metadata so the UI can distinguish "no edges" from "edges could not be
    read".
    """
    diagnostics: dict[str, Any] = {
        "edge_read_status": "ok",
        "edge_tables_scanned": 0,
        "edge_tables_failed": 0,
        "edge_errors": [],
    }
    if not node_ids:
        return [], diagnostics
    try:
        from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, REL_TYPES
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        rel_pairs = _relation_pairs(REL_TYPES, MULTI_REL_TYPES)
        cypher_executor = get_kg_registry().cypher_executor

        edges = []
        seen: set[tuple[str, str, str]] = set()  # (rel, src, tgt) dedup
        for rel_name, from_type, to_type in rel_pairs:
            diagnostics["edge_tables_scanned"] += 1
            try:
                result = cypher_executor.execute_read_only(
                    board_id,
                    f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
                    f"RETURN a.id, b.id, r.confidence "
                    f"LIMIT 5000",
                    max_rows=5000,
                )
                for row in result.get("rows", []):
                    src, tgt = row[0], row[1]
                    key = (rel_name, src, tgt)
                    if key in seen:
                        continue
                    # Pelo menos UMA ponta na página (era AND): com a projeção
                    # paginada, exigir ambas as pontas escondia quase toda edge.
                    # O cliente acumula as edges e materializa cada uma quando a
                    # outra ponta chega nas páginas seguintes.
                    if src in node_ids or tgt in node_ids:
                        seen.add(key)
                        edges.append({
                            "id": f"{src}-{rel_name}-{tgt}",
                            "source": src,
                            "target": tgt,
                            "edge_type": rel_name,
                            "confidence": row[2] if len(row) > 2 else 0.7,
                        })
            except Exception as exc:
                diagnostics["edge_tables_failed"] += 1
                diagnostics["edge_errors"].append({
                    "relationship": rel_name,
                    "from_type": from_type,
                    "to_type": to_type,
                    "error": str(exc),
                })
        if diagnostics["edge_tables_failed"]:
            diagnostics["edge_read_status"] = "partial_failure"
        diagnostics["edges_returned"] = len(edges)
        return edges, diagnostics
    except Exception as exc:
        logger.warning(
            "kg.graph.edge_read_failed board=%s err=%s",
            board_id, exc,
            extra={
                "event": "kg.graph.edge_read_failed",
                "board_id": board_id,
                "error": str(exc),
            },
        )
        diagnostics["edge_read_status"] = "failed"
        diagnostics["edge_tables_failed"] = diagnostics["edge_tables_scanned"]
        diagnostics["edge_errors"].append({"error": str(exc)})
        diagnostics["edges_returned"] = 0
        return [], diagnostics


def _relation_pairs(
    rel_types: list[tuple[str, str, str]],
    multi_rel_types: list[tuple[str, list[tuple[str, str]]]],
) -> list[tuple[str, str, str]]:
    rel_pairs: list[tuple[str, str, str]] = list(rel_types)
    for rel_name, pairs in multi_rel_types:
        for from_type, to_type in pairs:
            rel_pairs.append((rel_name, from_type, to_type))
    return rel_pairs


def _count_edges_by_type(board_id: str) -> tuple[dict[str, int], dict[str, Any]]:
    diagnostics: dict[str, Any] = {
        "edge_count_status": "ok",
        "edge_count_tables_scanned": 0,
        "edge_count_tables_failed": 0,
        "edge_count_errors": [],
    }
    try:
        from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, REL_TYPES
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        rel_names = sorted({
            rel_name
            for rel_name, *_ in _relation_pairs(REL_TYPES, MULTI_REL_TYPES)
        })
        cypher_executor = get_kg_registry().cypher_executor
        edge_counts: dict[str, int] = {}
        for rel_name in rel_names:
            diagnostics["edge_count_tables_scanned"] += 1
            try:
                result = cypher_executor.execute_read_only(
                    board_id,
                    f"MATCH ()-[r:{rel_name}]->() RETURN count(r) AS c",
                    max_rows=1,
                )
                rows = result.get("rows", [])
                count = int(rows[0][0]) if rows else 0
                if count:
                    edge_counts[rel_name] = count
            except Exception as exc:
                diagnostics["edge_count_tables_failed"] += 1
                diagnostics["edge_count_errors"].append({
                    "relationship": rel_name,
                    "error": str(exc),
                })
        if diagnostics["edge_count_tables_failed"]:
            diagnostics["edge_count_status"] = "partial_failure"
        return edge_counts, diagnostics
    except Exception as exc:
        logger.warning(
            "kg.stats.edge_count_failed board=%s err=%s",
            board_id, exc,
            extra={
                "event": "kg.stats.edge_count_failed",
                "board_id": board_id,
                "error": str(exc),
            },
        )
        diagnostics["edge_count_status"] = "failed"
        diagnostics["edge_count_errors"].append({"error": str(exc)})
        return {}, diagnostics


@router.get("/boards/{board_id}/similar")
async def find_similar(
    board_id: str,
    topic: str = "",
    top_k: int = Query(10, ge=1, le=50),
    min_similarity: float = 0.3,
):
    """Find similar decisions via semantic search."""
    if not topic:
        return _problem(400, "Bad Request", "topic query parameter is required")
    svc = get_kg_service()
    try:
        results = svc.find_similar_decisions(
            board_id, topic, top_k=top_k, min_similarity=min_similarity,
        )
        return {"results": results, "total": len(results)}
    except KGToolError as e:
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/supersedence/{decision_id}")
async def get_supersedence(board_id: str, decision_id: str):
    """Get supersedence chain for a decision node."""
    svc = get_kg_service()
    try:
        return svc.get_supersedence_chain(board_id, decision_id)
    except KGToolError as e:
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/contradictions")
async def find_contradictions(
    board_id: str,
    node_id: str = "",
    limit: int = Query(50, ge=1, le=200),
):
    """Find contradictions, optionally filtered by node_id."""
    svc = get_kg_service()
    try:
        results = svc.find_contradictions(
            board_id, node_id=node_id or None, max_rows=limit,
        )
        return {"contradictions": results, "total": len(results)}
    except KGToolError as e:
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/stats")
async def get_stats(
    board_id: str,
    min_relevance: float = Query(0.0, ge=0.0),
    graph_layer: str = "canonical",
):
    """Board KG stats: counts, confidence, pending."""
    svc = get_kg_service()
    try:
        layer = normalize_graph_layer(graph_layer)
        ver = svc.get_schema_version(board_id)
        all_nodes = svc.get_all_nodes(
            board_id,
            min_confidence=0.0,
            min_relevance=min_relevance,
            max_rows=1000,
            graph_layer=layer,
        )
        from okto_pulse.core.kg.schema_contract import NODE_TYPES

        node_counts: dict[str, int] = {
            node_type: svc.count_all_nodes(
                board_id,
                min_confidence=0.0,
                min_relevance=min_relevance,
                node_type=node_type,
                graph_layer=layer,
            )
            for node_type in NODE_TYPES
        }
        total_conf = 0.0
        total_relevance = 0.0
        for n in all_nodes:
            total_conf += float(n.get("source_confidence") or 0.0)
            total_relevance += float(n.get("relevance_score") or 0.0)
        edge_counts, edge_metadata = _count_edges_by_type(board_id)
        return {
            "schema_version": ver,
            "graph_schema_version": ver,
            "node_counts_by_type": node_counts,
            "edge_counts_by_type": edge_counts,
            "avg_confidence": round(total_conf / len(all_nodes), 2) if all_nodes else 0.0,
            "avg_relevance": (
                round(total_relevance / len(all_nodes), 4) if all_nodes else 0.0
            ),
            "pending_queue_count": 0,
            "last_consolidation_at": None,
            "min_relevance": min_relevance,
            "graph_layer": layer,
            **edge_metadata,
        }
    except KGToolError as e:
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/metrics")
async def get_kg_metrics(board_id: str):
    """Provenance metrics for the KG v0.2.0 pipeline — grouped by `layer`.

    Returns:
    - edge_count_by_layer: {deterministic, cognitive, fallback, legacy} → int
    - deterministic_edge_ratio, cognitive_edge_ratio, fallback_edge_ratio: float 0-1
    - node_count_by_type: histogram by KG NODE_TYPES
    - edges_total / nodes_total

    Health targets (spec c48a5c33 Analysis section):
    - deterministic_edge_ratio ≥ 0.70
    - fallback_edge_ratio ≤ 0.15
    - cognitive_edge_ratio 0.15 – 0.30 in mature boards
    """
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, REL_TYPES

    if not get_kg_registry().graph_path_resolver.exists(board_id):
        return {
            "board_id": board_id,
            "kg_bootstrapped": False,
            "edge_count_by_layer": {},
            "deterministic_edge_ratio": 0.0,
            "cognitive_edge_ratio": 0.0,
            "fallback_edge_ratio": 0.0,
            "edges_total": 0,
            "nodes_total": 0,
            "node_count_by_type": {},
        }

    edge_count_by_layer: dict[str, int] = {
        "deterministic": 0, "cognitive": 0, "fallback": 0, "legacy": 0,
        "unknown": 0,
    }
    edge_by_rule: dict[str, int] = {}
    node_count_by_type: dict[str, int] = {}

    # Iterate every rel name — single-pair (REL_TYPES) AND multi-pair
    # (MULTI_REL_TYPES, e.g. `belongs_to` hierarchy backbone). Without the
    # MULTI_REL_TYPES pass, the metrics page silently under-counts ~80% of
    # the deterministic edges Layer 1 produces.
    all_rel_names = [r[0] for r in REL_TYPES] + [m[0] for m in MULTI_REL_TYPES]
    # R05-C: read through the #06 GraphTransaction port (scope.execute) instead
    # of the direct board-connection tuple — the DB handle was unused and every
    # statement is a plain scope.execute, so the swap is behaviour-identical.
    async with await get_kg_registry().graph_transaction.begin(board_id) as scope:
        for rel_name in all_rel_names:
            try:
                # Kùzu groups implicitly on non-aggregate projections, but
                # tolerates NULL only when we pre-coalesce per-row. Returning
                # raw rows and aggregating in Python keeps the code portable
                # across Kùzu versions (GROUP BY syntax shifted between 0.6
                # and 0.11).
                result = scope.execute(
                    f"MATCH ()-[r:{rel_name}]->() "
                    f"RETURN r.layer, r.rule_id"
                )
            except Exception:
                continue
            while result.has_next():
                row = result.get_next()
                layer = (row[0] or "unknown")
                rule_id = (row[1] or "")
                edge_count_by_layer[layer] = edge_count_by_layer.get(layer, 0) + 1
                if rule_id:
                    edge_by_rule[rule_id] = edge_by_rule.get(rule_id, 0) + 1

        # Node type histogram — aggregate per type to dodge GROUP BY portability.
        from okto_pulse.core.kg.schema_contract import NODE_TYPES
        for nt in NODE_TYPES:
            try:
                result = scope.execute(
                    f"MATCH (n:{nt}) RETURN count(n) AS c"
                )
                if result.has_next():
                    c = int(result.get_next()[0])
                    if c:
                        node_count_by_type[nt] = c
            except Exception:
                continue

    edges_total = sum(edge_count_by_layer.values())
    nodes_total = sum(node_count_by_type.values())

    def _ratio(num: int, den: int) -> float:
        return round(num / den, 3) if den else 0.0

    return {
        "board_id": board_id,
        "kg_bootstrapped": True,
        "edge_count_by_layer": edge_count_by_layer,
        "edge_count_by_rule": edge_by_rule,
        "deterministic_edge_ratio": _ratio(
            edge_count_by_layer["deterministic"], edges_total,
        ),
        "cognitive_edge_ratio": _ratio(
            edge_count_by_layer["cognitive"], edges_total,
        ),
        "fallback_edge_ratio": _ratio(
            edge_count_by_layer["fallback"], edges_total,
        ),
        "legacy_edge_ratio": _ratio(
            edge_count_by_layer["legacy"], edges_total,
        ),
        "edges_total": edges_total,
        "nodes_total": nodes_total,
        "node_count_by_type": node_count_by_type,
        "health_targets": {
            "deterministic_edge_ratio_min": 0.70,
            "fallback_edge_ratio_max": 0.15,
            "cognitive_edge_ratio_target_range": [0.15, 0.30],
        },
    }


@router.get("/boards/{board_id}/audit")
async def list_audit(
    board_id: str,
    limit: int = Query(50, ge=1, le=200),
    cursor: str = "",
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """List consolidation audit entries."""
    result = await ListAuditUseCase().execute(
        ListAuditCommand(board_id, limit=limit),
        actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
        uow=uow,
    )
    return {"entries": result.entries, "next_cursor": None}


@router.post("/boards/{board_id}/audit/{session_id}/undo")
async def undo_session(board_id: str, session_id: str, force: bool = False):
    """Undo a consolidation session."""
    return _problem(501, "Not Implemented", "Undo will be available in governance sprint")


@router.get("/boards/{board_id}/audit/export")
async def export_audit(
    board_id: str,
    format: str = Query("json", pattern="^(json|csv)$"),
):
    """Streaming audit export as JSONL or CSV."""
    async def _stream():
        if format == "json":
            yield '{"entries": []}\n'
        else:
            yield "session_id,board_id,committed_at\n"

    content_type = "application/jsonl" if format == "json" else "text/csv"
    return StreamingResponse(
        _stream(),
        media_type=content_type,
        headers={"Content-Disposition": f"attachment; filename=audit_{board_id}.{format}"},
    )


@router.get("/global/search")
async def global_search(
    q: str = "",
    limit: int = Query(20, ge=1, le=100),
    min_similarity: float = Query(0.3, ge=0.0, le=1.0),
    graph_layer: str = "canonical",
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Cross-board global discovery search.

    For community edition, searches across all boards since auth is local-only.
    For production, would filter by user's accessible boards.
    """
    try:
        result = await GlobalSearchUseCase().execute(
            GlobalSearchCommand(
                q=q,
                limit=limit,
                min_similarity=min_similarity,
                graph_layer=graph_layer,
            ),
            actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
            uow=uow,
        )
        return {
            "results": result.results,
            "total": len(result.results),
            "graph_layer": result.graph_layer,
        }
    except KGToolError as e:
        return _handle_kg_error(e)


@router.post("/boards/{board_id}/historical-consolidation/start")
async def start_historical(
    board_id: str, uow: PulseUnitOfWork = Depends(get_unit_of_work)
):
    """Start historical backfill."""
    result = await StartHistoricalUseCase().execute(
        StartHistoricalCommand(board_id),
        actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
        uow=uow,
    )
    return result.payload


@router.post("/boards/{board_id}/historical-consolidation/cancel")
async def cancel_historical_endpoint(
    board_id: str, uow: PulseUnitOfWork = Depends(get_unit_of_work)
):
    """Cancel historical backfill."""
    result = await CancelHistoricalUseCase().execute(
        CancelHistoricalCommand(board_id),
        actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
        uow=uow,
    )
    return result.payload


@router.get("/boards/{board_id}/historical-consolidation/progress")
async def historical_progress_endpoint(
    board_id: str, uow: PulseUnitOfWork = Depends(get_unit_of_work)
):
    """Historical consolidation progress."""
    result = await GetHistoricalProgressUseCase().execute(
        GetHistoricalProgressCommand(board_id),
        actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
        uow=uow,
    )
    return result.progress


@router.delete("/boards/{board_id}/kg")
async def delete_board_kg(
    board_id: str, uow: PulseUnitOfWork = Depends(get_unit_of_work)
):
    """Wipe KG data for a board (right-to-erasure)."""
    await DeleteBoardKgUseCase().execute(
        DeleteBoardKgCommand(board_id),
        actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
        uow=uow,
    )
    return Response(status_code=204)


def _describe_embedding_provider(provider: Any) -> dict[str, Any]:
    """Introspect the registered embedding provider WITHOUT triggering a load.

    Delegates to the metadata-driven describer in the embedding port (R13-A):
    this common API surface no longer imports or ``isinstance``-checks
    concrete provider classes — provider description is driven by capability
    metadata. Reads ``_model`` directly (never calls
    ``_get_model()``) so /kg/settings can report the live state for a health
    banner without paying the model-load cost. See TR-4 of spec
    `sentence-transformers como dep obrigatoria` and R13-A fr_r13a_provider_metadata.
    """
    from okto_pulse.core.kg.interfaces.embedding import describe_embedding_provider

    return describe_embedding_provider(provider)


@router.get("/settings")
async def get_global_kg_settings():
    """Return process-global KG settings (no board context required).

    Exposes the embedding-provider state so the Dashboard Settings banner and
    smoke-test tooling can tell stub-mode apart from a healthy load without
    needing to pick a board. MUST NOT trigger a model load (TR-4).
    """
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    registry = get_kg_registry()
    config = registry.config
    payload = {
        "graph_store": type(registry.graph_store).__name__ if registry.graph_store else None,
        "session_ttl_seconds": config.kg_session_ttl_seconds if config else 900,
        "kg_base_dir": str(config.kg_base_dir) if config else None,
    }
    payload.update(_describe_embedding_provider(registry.embedding_provider))
    return payload


@router.get("/boards/{board_id}/settings")
async def get_settings(
    board_id: str, uow: PulseUnitOfWork = Depends(get_unit_of_work)
):
    """Get KG settings for a board."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    registry = get_kg_registry()
    config = registry.config
    kg_exists = registry.graph_path_resolver.exists(board_id)

    # Check historical consolidation status (the only relational read in this
    # handler; the registry/embedding introspection below is not DB-bound).
    progress = (
        await GetHistoricalProgressUseCase().execute(
            GetHistoricalProgressCommand(board_id),
            actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
            uow=uow,
        )
    ).progress

    payload = {
        "consolidation_enabled": True,
        "enable_historical_consolidation": progress.get("enabled", False),
        "kg_initialized": kg_exists,
        # Preserved for backwards compatibility with older clients.
        "embedding_provider": type(registry.embedding_provider).__name__,
        "graph_store": type(registry.graph_store).__name__ if registry.graph_store else None,
        "session_ttl_seconds": config.kg_session_ttl_seconds if config else 900,
        "kg_base_dir": str(config.kg_base_dir) if config else None,
    }
    payload.update(_describe_embedding_provider(registry.embedding_provider))
    return payload


@router.put("/boards/{board_id}/settings")
async def update_settings(board_id: str, request: Request):
    """Update KG settings for a board."""
    return {"success": True}


@router.post("/boards/{board_id}/cypher")
async def cypher_query(board_id: str, cypher: str = "", params: dict | None = None,
                       max_rows: int = 1000, timeout_ms: int = 5000,
                       include_working: bool = False):
    """Delegate to tier power query_cypher."""
    try:
        result = execute_cypher_read_only(
            board_id,
            cypher,
            params,
            max_rows=max_rows,
            timeout_ms=timeout_ms,
            include_working=include_working,
        )
        return result
    except TierPowerError as e:
        return _problem(
            400 if e.code in ("unsafe_cypher", "invalid_cypher") else 503,
            e.code,
            e.message,
            e.code,
        )


@router.get("/schema")
async def schema_info(board_id: str = "", include_internal: bool = False):
    """Schema introspection."""
    result = get_schema_info(board_id or "default", include_internal=include_internal)
    return result


@router.get("/boards/{board_id}/pending")
async def list_pending(
    board_id: str, uow: PulseUnitOfWork = Depends(get_unit_of_work)
):
    """List pending consolidation queue entries."""
    try:
        result = await ListPendingUseCase().execute(
            ListPendingCommand(board_id),
            actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
            uow=uow,
        )
        return {"entries": result.entries, "count": len(result.entries)}
    except Exception:
        return {"entries": [], "count": 0}


@router.get("/boards/{board_id}/pending/tree")
async def list_pending_tree(
    board_id: str,
    depth: int = Query(5, ge=1, le=5),
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Hierarchical pending-queue view (spec f33eb9ca — Layer 4 Pending Queue UI).

    Returns a 5-level tree: Ideations → Refinements → Specs → Sprints →
    Cards, each level annotated with aggregate status counters drawn from
    `consolidation_queue`. The UI renders this via
    `frontend/src/components/knowledge/PendingQueueTree.tsx` with lazy
    expansion by level (BR `Tree Lazy Fetch por Nível`).

    Payload shape (stable — consumed by React component):
        {
          "board_id": str,
          "total_pending": int,
          "levels": {ideations: {pending,in_progress,done,failed},
                     refinements: ..., specs: ..., sprints: ..., cards: ...},
          "tree": [ideation-nodes with nested children]
        }

    The queue-state fetch + in-Python hierarchy join lives in
    ``kg/dashboard_readers.build_pending_tree`` (a service reader) so this
    endpoint holds no relational symbol; the use case returns the payload verbatim.
    """
    result = await ListPendingTreeUseCase().execute(
        ListPendingTreeCommand(board_id, depth=depth),
        actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
        uow=uow,
    )
    return result.payload


# ---------------------------------------------------------------------------
# SSE live-events stream (spec f33eb9ca — useKgLiveEvents hook)
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/events")
async def stream_kg_events(
    board_id: str,
    since: str | None = Query(None, description="ISO timestamp — only emit events created after this"),
):
    """Server-Sent Events stream of `kg.session.committed` /
    `kg.board.cleared` events for the given board.

    This endpoint is the backing data source for the React `useKgLiveEvents`
    hook (Frontend responsibility — hook implementation lives in the sibling
    UI repo).

    Protocol:
        event: kg.session.committed
        data: {"event_id": ..., "session_id": ..., "payload": {...}}

    Filter by `since` to resume a dropped connection without replaying the
    entire outbox backlog. Absence ⇒ start from now().

    Root-cause fix (pool leak → exaustão → "travamento", 2026-06-09): o
    generator NÃO toca mais no DB. O polling do outbox + queue snapshot vive
    no :mod:`okto_pulse.core.api.kg_events_hub` — uma task em background por
    board, com fan-out para uma ``asyncio.Queue`` por assinante. A versão
    anterior abria uma sessão por iteração DENTRO do generator; quando o
    cliente desconectava, o hard-cancel da request interrompia o close da
    conexão e ela nunca voltava ao pool. O único acesso a DB remanescente
    aqui é o replay de reconexão (``since``), feito via
    ``cancel_safe_session`` (close blindado contra cancelamento).
    """
    import asyncio as _asyncio
    from datetime import datetime as _dt, timezone as _tz

    from okto_pulse.core.api.kg_events_hub import (
        format_outbox_row_sse,
        format_progress_sse,
        get_kg_events_hub,
        query_outbox_rows,
    )
    from okto_pulse.core.infra.database import cancel_safe_session

    try:
        since_cursor = _dt.fromisoformat(since) if since else None
    except ValueError:
        raise HTTPException(status_code=400, detail="since must be ISO 8601")

    def _as_utc(value: _dt) -> _dt:
        return value if value.tzinfo is not None else value.replace(tzinfo=_tz.utc)

    hub = get_kg_events_hub()
    subscription = hub.subscribe(board_id)

    async def _iter():
        try:
            # Initial heartbeat so the client knows the connection is alive.
            yield "event: hello\ndata: {}\n\n"

            # Replay de reconexão: eventos em (since, cursor-do-hub] saem
            # daqui; eventos posteriores ao cursor chegam pela queue do hub
            # (sem gap nem duplicata). Best-effort — nunca quebra o stream.
            if since_cursor is not None:
                try:
                    async with cancel_safe_session() as session:
                        backlog = await query_outbox_rows(
                            session, board_id, since_cursor, limit=500,
                        )
                    for row in backlog:
                        raw = row.pop("_created_at_raw")
                        if raw is not None and _as_utc(raw) > _as_utc(subscription.cursor):
                            break  # daqui em diante a queue do hub entrega
                        yield format_outbox_row_sse(row)
                except Exception:
                    pass

            # Snapshot imediato quando o hub já conhece o estado da queue;
            # para um board recém-assinado o primeiro ciclo do poller emite
            # o snapshot em ≤1s.
            if subscription.initial_progress is not None:
                yield format_progress_sse(subscription.initial_progress)

            while True:
                try:
                    chunk = await _asyncio.wait_for(
                        subscription.queue.get(), timeout=15.0,
                    )
                except _asyncio.TimeoutError:
                    # Keepalive comment so proxies don't drop the connection
                    # in between committed bursts.
                    yield ": keepalive\n\n"
                    continue
                yield chunk
        finally:
            # Roda tanto no fechamento normal (aclose/GeneratorExit) quanto
            # no hard-cancel — unsubscribe é síncrono e não toca em DB.
            hub.unsubscribe(subscription)

    return StreamingResponse(_iter(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # disables nginx buffering
    })


# ---------------------------------------------------------------------------
# Retry-from-here on a pending queue entry (spec f33eb9ca)
# ---------------------------------------------------------------------------


@router.post("/boards/{board_id}/pending/{queue_entry_id}/retry")
async def retry_pending_entry(
    board_id: str,
    queue_entry_id: str,
    recursive: bool = Query(False, description="Also re-enqueue descendant entries"),
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Re-queue a failed/done ConsolidationQueue entry so the worker
    reprocesses it. `recursive=true` also re-enqueues descendants below
    the artifact in the Ideation→Refinement→Spec→Sprint→Card hierarchy.

    Idempotency: content_hash BR still owns "nothing actually changed"
    no-op behaviour downstream, so retrying an unchanged artifact is a
    cheap round-trip that touches the outbox once.

    The mutation + recursive sweep + commit + worker signal live in
    ``kg/governance.retry_pending_entry`` (a service write); a missing entry comes
    back as ``EntityNotFoundError`` which this adapter maps to the legacy 404
    ("queue entry not found").
    """
    try:
        result = await RetryPendingEntryUseCase().execute(
            RetryPendingEntryCommand(board_id, queue_entry_id, recursive=recursive),
            actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
            uow=uow,
        )
    except EntityNotFoundError as exc:
        raise RESTAdapterContract.http_error(
            exc, not_found_detail="queue entry not found"
        )
    return result.payload


@router.post("/boards/{board_id}/nodes/{node_id}/boost")
async def boost_node(
    board_id: str,
    node_id: str,
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Increment a node's ``relevance_score`` by a fixed +0.3 with clamp [0, 1.5].

    Persists a ``ConsolidationAudit`` row for the boost with all required NOT-NULL
    columns populated (``artifact_type="boost"``, ``started_at``, …) — bug 547a2aa8
    fix; the legacy row omitted ``artifact_type``/``started_at`` so its commit always
    raised IntegrityError and was silently swallowed (200 with no audit row). Each
    boost's audit row has a unique PK (uuid-suffixed ``session_id``) so repeated
    boosts of the same node persist distinct rows. Idempotency is NOT enforced —
    each call stacks another +0.3 until the clamp is reached, by design (repeat
    clicks should reflect repeat intent).

    Responses:
        200 — `{node_id, node_type, score_before, score_after, boosted_at, boosted_by}`
        404 — node not found in any table of the per-board graph

    The graph read/SET + ``ConsolidationAudit`` staging live in
    ``kg.governance.boost_node``; the ``BoostNodeUseCase`` commits the staged audit
    via the UnitOfWork (best-effort — a commit failure on the already-boosted graph
    rolls back the audit-only row and the boost still returns 200, preserving the
    legacy contract). A missing node comes back as ``EntityNotFoundError`` (mapped to
    the legacy 404 problem) and a failed SET as ``BoostPersistError`` (mapped to the
    legacy 500 ``kuzu_error`` problem).
    """
    from okto_pulse.core.kg.governance import BoostPersistError

    try:
        result = await BoostNodeUseCase().execute(
            BoostNodeCommand(board_id, node_id),
            actor=RESTAdapterContract.actor(_KG_ACTOR_ID),
            uow=uow,
        )
    except EntityNotFoundError:
        return _problem(
            status=404,
            title="Node not found",
            detail=f"Node {node_id} not present in board {board_id}",
            error_type="not_found",
        )
    except BoostPersistError as exc:
        return _problem(
            status=500,
            title="Boost persist failed",
            detail=str(exc),
            error_type="kuzu_error",
        )
    return result.payload


@router.get("/openapi.json")
async def openapi_spec():
    """Auto-generated OpenAPI 3.1 spec."""
    return {"info": {"title": "Okto Pulse KG API", "version": "0.1.0"}}


# ---------------------------------------------------------------------------
# Schema migration self-heal (spec 818748f2 — FR5)
# ---------------------------------------------------------------------------


class MigrateSchemaResponse(BaseModel):
    board_id: str
    migrated: bool = Field(
        ..., description="True if the migration completed without errors."
    )
    columns_added: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Per-node-type list of columns ALTER ADDed this run.",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="Non-fatal warnings collected during migration.",
    )
    duration_ms: int


@router.post(
    "/{board_id}/migrate-schema",
    response_model=MigrateSchemaResponse,
)
async def post_migrate_schema(board_id: str):
    """Force-apply schema migrations for a board (idempotent).

    Use when consolidation fails with `Binder exception: Cannot find
    property X for n` — usually means an ALTER ADD migration was missed
    for a board bootstrapped before that schema column was introduced.

    Re-runs all v0.3.x ALTER TABLE ADD on every node type. Idempotent:
    calling on a board already migrated returns ``migrated=true`` with
    ``columns_added`` empty (no-op).

    Spec 818748f2.
    """
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    summary = await get_kg_registry().graph_schema_manager.migrate(board_id)
    if not summary["migrated"] and any(
        "board_not_found" in e for e in summary["errors"]
    ):
        return _problem(
            status=404,
            title="Board not found",
            detail=summary["errors"][0],
            error_type="not_found",
        )
    return MigrateSchemaResponse(**summary)
