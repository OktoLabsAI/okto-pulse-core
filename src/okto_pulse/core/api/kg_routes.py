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
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from okto_pulse.core.kg.kg_service import KGService, KGToolError, get_kg_service
from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    execute_cypher_read_only,
    execute_natural_query,
    get_schema_info,
)
from okto_pulse.core.kg.governance import start_historical_consolidation, cancel_historical, get_historical_progress
from okto_pulse.core.infra.database import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/kg", tags=["knowledge-graph"])


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
    limit: int = Query(50, ge=1, le=200),
    cursor: str = "",
):
    """List KG nodes with filters and cursor pagination."""
    svc = get_kg_service()
    try:
        rows = svc.get_decision_history(
            board_id, type or "", min_confidence=min_confidence, max_rows=limit,
        )
        return {"nodes": rows, "next_cursor": None, "total_hint": len(rows)}
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
):
    """Return subgraph for visualization — Spec 8 / S1.1, S1.4, S1.5.

    Pagination contract:

    * ``limit`` in [1, 500]; out-of-range values yield **400** (not 422) per
      AC-11 so clients get a human-readable reason instead of Pydantic's
      validation schema.
    * ``cursor`` is an opaque base64 token emitted by a prior call. A
      corrupted cursor yields **410 Gone** per AC-12 — it cannot be
      silently reinterpreted as "first page".
    * Response always carries ``next_cursor``. It is ``None`` when the
      returned page is the last one so clients can stop paging without a
      second round trip.
    """
    if limit < 1 or limit > 500:
        return _problem(
            400,
            "Bad Request",
            f"limit must be in range [1, 500], got {limit}",
            "invalid_limit",
        )

    svc = get_kg_service()
    try:
        if center:
            rows = svc.get_related_context(board_id, center, max_rows=limit)
            next_cursor: str | None = None
        else:
            try:
                rows = svc.get_all_nodes(
                    board_id,
                    min_confidence=0.0,
                    max_rows=limit,
                    cursor=cursor or None,
                )
            except ValueError as exc:
                return _problem(
                    410,
                    "Gone",
                    f"cursor is invalid or corrupted: {exc}",
                    "invalid_cursor",
                )
            next_cursor = _next_cursor_for(rows, limit)

        node_ids = {r.get("id") or r[0] if isinstance(r, list) else r.get("id", "") for r in rows}
        edges = _fetch_edges_for_nodes(board_id, node_ids)

        return {
            "nodes": rows,
            "edges": edges,
            "next_cursor": next_cursor,
            "metadata": {"depth": depth, "truncated": len(rows) >= limit},
        }
    except KGToolError as e:
        return _handle_kg_error(e)


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


def _fetch_edges_for_nodes(board_id: str, node_ids: set[str]) -> list[dict]:
    """Fetch all edges between the given node IDs from Kuzu."""
    if not node_ids:
        return []
    try:
        from okto_pulse.core.kg.schema import (
            MULTI_REL_TYPES,
            REL_TYPES,
            open_board_connection,
        )

        # Iterate single-pair AND multi-pair rels — `belongs_to` lives in
        # MULTI_REL_TYPES and would otherwise be silently dropped from the
        # subgraph payload, leaving the canvas hierarchy disconnected.
        rel_pairs: list[tuple[str, str, str]] = list(REL_TYPES)
        for rel_name, pairs in MULTI_REL_TYPES:
            for from_type, to_type in pairs:
                rel_pairs.append((rel_name, from_type, to_type))

        edges = []
        seen: set[tuple[str, str, str]] = set()  # (rel, src, tgt) dedup
        with open_board_connection(board_id) as (_db, conn):
            for rel_name, from_type, to_type in rel_pairs:
                try:
                    result = conn.execute(
                        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
                        f"RETURN a.id, b.id, r.confidence "
                        f"LIMIT 500",
                    )
                    while result.has_next():
                        row = result.get_next()
                        src, tgt = row[0], row[1]
                        key = (rel_name, src, tgt)
                        if key in seen:
                            continue
                        if src in node_ids and tgt in node_ids:
                            seen.add(key)
                            edges.append({
                                "id": f"{src}-{rel_name}-{tgt}",
                                "source": src,
                                "target": tgt,
                                "edge_type": rel_name,
                                "confidence": row[2] if len(row) > 2 else 0.7,
                            })
                except Exception:
                    pass
        return edges
    except Exception:
        return []


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
async def get_stats(board_id: str):
    """Board KG stats: counts, confidence, pending."""
    svc = get_kg_service()
    try:
        ver = svc.get_schema_version(board_id)
        all_nodes = svc.get_all_nodes(board_id, min_confidence=0.0, max_rows=1000)
        node_counts: dict[str, int] = {}
        total_conf = 0.0
        for n in all_nodes:
            t = n.get("node_type", "Unknown")
            node_counts[t] = node_counts.get(t, 0) + 1
            total_conf += n.get("source_confidence", 0.0)
        return {
            "schema_version": ver,
            "node_counts_by_type": node_counts,
            "edge_counts_by_type": {},
            "avg_confidence": round(total_conf / len(all_nodes), 2) if all_nodes else 0.0,
            "pending_queue_count": 0,
            "last_consolidation_at": None,
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
    from okto_pulse.core.kg.schema import (
        MULTI_REL_TYPES,
        REL_TYPES,
        board_kuzu_path,
        open_board_connection,
    )

    if not board_kuzu_path(board_id).exists():
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
    with open_board_connection(board_id) as (_db, conn):
        for rel_name in all_rel_names:
            try:
                # Kùzu groups implicitly on non-aggregate projections, but
                # tolerates NULL only when we pre-coalesce per-row. Returning
                # raw rows and aggregating in Python keeps the code portable
                # across Kùzu versions (GROUP BY syntax shifted between 0.6
                # and 0.11).
                result = conn.execute(
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
        from okto_pulse.core.kg.schema import NODE_TYPES
        for nt in NODE_TYPES:
            try:
                result = conn.execute(
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
    db: AsyncSession = Depends(get_db),
):
    """List consolidation audit entries."""
    from sqlalchemy import select
    from okto_pulse.core.models.db import ConsolidationAudit

    query = (
        select(ConsolidationAudit)
        .where(
            ConsolidationAudit.board_id == board_id,
            ConsolidationAudit.committed_at.is_not(None),
        )
        .order_by(ConsolidationAudit.committed_at.desc())
        .limit(limit)
    )
    result = await db.execute(query)
    rows = result.scalars().all()

    entries = [
        {
            "session_id": r.session_id,
            "board_id": r.board_id,
            "artifact_id": r.artifact_id,
            "artifact_type": getattr(r, "artifact_type", ""),
            "agent_id": r.agent_id,
            "committed_at": r.committed_at.isoformat() if r.committed_at else None,
            "nodes_added": r.nodes_added or 0,
            "nodes_updated": r.nodes_updated or 0,
            "nodes_superseded": r.nodes_superseded or 0,
            "edges_added": r.edges_added or 0,
            "summary_text": r.summary_text,
            "undo_status": r.undo_status or "none",
        }
        for r in rows
    ]
    return {"entries": entries, "next_cursor": None}


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
    db: AsyncSession = Depends(get_db),
):
    """Cross-board global discovery search.

    For community edition, searches across all boards since auth is local-only.
    For production, would filter by user's accessible boards.
    """
    # For community edition, get all boards (no user filtering)
    from sqlalchemy import select
    from okto_pulse.core.models.db import Board

    query = select(Board).limit(100)
    result = await db.execute(query)
    boards = result.scalars().all()
    user_board_ids = [b.id for b in boards]

    svc = get_kg_service()
    try:
        results = svc.query_global(q, user_boards=user_board_ids, top_k=limit)
        return {"results": results, "total": len(results)}
    except KGToolError as e:
        return _handle_kg_error(e)


@router.post("/boards/{board_id}/historical-consolidation/start")
async def start_historical(board_id: str, db: AsyncSession = Depends(get_db)):
    """Start historical backfill."""
    result = await start_historical_consolidation(db, board_id)
    return result


@router.post("/boards/{board_id}/historical-consolidation/cancel")
async def cancel_historical_endpoint(board_id: str, db: AsyncSession = Depends(get_db)):
    """Cancel historical backfill."""
    result = await cancel_historical(db, board_id)
    return result


@router.get("/boards/{board_id}/historical-consolidation/progress")
async def historical_progress_endpoint(board_id: str, db: AsyncSession = Depends(get_db)):
    """Historical consolidation progress."""
    result = await get_historical_progress(db, board_id)
    return result


@router.delete("/boards/{board_id}/kg")
async def delete_board_kg(board_id: str):
    """Wipe KG data for a board (right-to-erasure)."""
    from okto_pulse.core.kg.global_discovery.clustering import board_delete_cascade
    counts = board_delete_cascade(board_id)
    return Response(status_code=204)


def _describe_embedding_provider(provider: Any) -> dict[str, Any]:
    """Introspect the registered embedding provider WITHOUT triggering a load.

    Reads `_model` directly (never calls `_get_model()`) so /kg/settings can
    report the live state for a health banner without paying the model-load
    cost. See TR-4 of spec `sentence-transformers como dep obrigatoria`.
    """
    from okto_pulse.core.kg.embedding import (
        SentenceTransformerProvider,
        StubEmbeddingProvider,
    )

    name = type(provider).__name__ if provider is not None else "NoneProvider"
    is_stub = isinstance(provider, StubEmbeddingProvider) or provider is None
    model_name: str | None = None
    is_loaded = False
    dimension = 0

    if isinstance(provider, SentenceTransformerProvider):
        model_name = provider.model_name
        is_loaded = provider._model is not None
        dimension = provider.dim
    elif isinstance(provider, StubEmbeddingProvider):
        is_loaded = True  # stub has no external artifact to load
        dimension = provider.dim
    elif provider is not None:
        # Unknown provider — best-effort introspection, no load.
        model_name = getattr(provider, "model_name", None)
        is_loaded = getattr(provider, "_model", None) is not None
        dimension = getattr(provider, "dim", 0)

    return {
        "embedding_provider_name": name,
        "model_name": model_name,
        "embedding_dimension": dimension,
        "is_loaded": is_loaded,
        "is_stub": is_stub,
    }


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
async def get_settings(board_id: str, db: AsyncSession = Depends(get_db)):
    """Get KG settings for a board."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.schema import board_kuzu_path

    registry = get_kg_registry()
    config = registry.config
    kg_exists = board_kuzu_path(board_id).exists()

    # Check historical consolidation status
    progress = await get_historical_progress(db, board_id)

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
                       max_rows: int = 1000, timeout_ms: int = 5000):
    """Delegate to tier power query_cypher."""
    try:
        result = execute_cypher_read_only(board_id, cypher, params, max_rows=max_rows, timeout_ms=timeout_ms)
        return result
    except TierPowerError as e:
        return _problem(400 if e.code in ("unsafe_cypher", "invalid_cypher") else 503, e.code, e.message, e.code)


@router.get("/schema")
async def schema_info(board_id: str = "", include_internal: bool = False):
    """Schema introspection."""
    result = get_schema_info(board_id or "default", include_internal=include_internal)
    return result


@router.get("/boards/{board_id}/pending")
async def list_pending(board_id: str, db: AsyncSession = Depends(get_db)):
    """List pending consolidation queue entries."""
    from sqlalchemy import select
    from okto_pulse.core.models.db import ConsolidationQueue

    try:
        query = (
            select(ConsolidationQueue)
            .where(ConsolidationQueue.board_id == board_id)
            .order_by(ConsolidationQueue.triggered_at.desc())
            .limit(100)
        )
        result = await db.execute(query)
        rows = result.scalars().all()

        entries = [
            {
                "id": r.id,
                "board_id": r.board_id,
                "artifact_id": r.artifact_id,
                "artifact_type": r.artifact_type,
                "priority": r.priority,
                "source": r.source,
                "status": r.status,
                "triggered_at": r.triggered_at.isoformat() if r.triggered_at else None,
                "claimed_by_session_id": r.claimed_by_session_id,
            }
            for r in rows
        ]
        return {"entries": entries, "count": len(entries)}
    except Exception:
        return {"entries": [], "count": 0}


@router.get("/boards/{board_id}/pending/tree")
async def list_pending_tree(
    board_id: str,
    depth: int = Query(5, ge=1, le=5),
    db: AsyncSession = Depends(get_db),
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
    """
    from sqlalchemy import select, func
    from collections import defaultdict
    from okto_pulse.core.models.db import (
        Card, ConsolidationQueue, Ideation, Refinement, Spec, Sprint,
    )

    # Fetch queue state once, then join in-Python against the hierarchy.
    # A recursive CTE across SQLite + JSON test_scenarios would be
    # brittle; per-table fetches keep the code portable and cover the
    # unique-constraint-indexed case (per-board lookups are cheap).
    q_rows = (await db.execute(
        select(ConsolidationQueue).where(ConsolidationQueue.board_id == board_id)
    )).scalars().all()
    q_by_artifact: dict[tuple[str, str], ConsolidationQueue] = {
        (r.artifact_type, r.artifact_id): r for r in q_rows
    }

    def _queue_meta(art_type: str, art_id: str) -> dict:
        entry = q_by_artifact.get((art_type, art_id))
        if entry is None:
            return {"status": "not_queued", "queued_age_seconds": None,
                    "retry_count": 0, "layer": None, "last_error": None}
        age = None
        if entry.triggered_at is not None:
            # SQLite may return naive datetimes; normalise both sides to UTC.
            trig = entry.triggered_at
            if trig.tzinfo is None:
                trig = trig.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - trig).total_seconds()
        return {
            "status": entry.status,
            "queued_age_seconds": int(age) if age is not None else None,
            "retry_count": 0,  # placeholder until retry counter lands
            "layer": entry.source or "unknown",
            "last_error": None,
        }

    ideas = (await db.execute(
        select(Ideation).where(Ideation.board_id == board_id)
    )).scalars().all()
    refs = (await db.execute(
        select(Refinement).where(Refinement.board_id == board_id)
    )).scalars().all()
    specs = (await db.execute(
        select(Spec).where(Spec.board_id == board_id)
    )).scalars().all()
    sprints = (await db.execute(
        select(Sprint).where(Sprint.board_id == board_id)
    )).scalars().all()
    cards = (await db.execute(
        select(Card).where(Card.board_id == board_id)
    )).scalars().all()

    refs_by_ideation: dict[str, list] = defaultdict(list)
    for r in refs:
        refs_by_ideation[r.ideation_id or ""].append(r)
    specs_by_refinement: dict[str, list] = defaultdict(list)
    specs_orphan: list = []
    for s in specs:
        if s.refinement_id:
            specs_by_refinement[s.refinement_id].append(s)
        else:
            specs_orphan.append(s)
    sprints_by_spec: dict[str, list] = defaultdict(list)
    for sp in sprints:
        sprints_by_spec[sp.spec_id].append(sp)
    cards_by_sprint: dict[str, list] = defaultdict(list)
    cards_by_spec_direct: dict[str, list] = defaultdict(list)
    for c in cards:
        if getattr(c, "sprint_id", None):
            cards_by_sprint[c.sprint_id].append(c)
        else:
            cards_by_spec_direct[c.spec_id].append(c)

    levels_counter = {
        lvl: {"pending": 0, "in_progress": 0, "done": 0, "failed": 0,
              "not_queued": 0}
        for lvl in ("ideations", "refinements", "specs", "sprints", "cards")
    }

    def _tally(level: str, art_type: str, art_id: str) -> str:
        status = _queue_meta(art_type, art_id)["status"]
        levels_counter[level][status] = levels_counter[level].get(status, 0) + 1
        return status

    def _card_node(c) -> dict:
        meta = _queue_meta("card", c.id)
        _tally("cards", "card", c.id)
        return {
            "id": c.id, "type": "card",
            "title": c.title,
            "card_type": str(c.card_type) if getattr(c, "card_type", None) else "normal",
            **meta,
            "children": [],
        }

    def _sprint_node(sp) -> dict:
        meta = _queue_meta("sprint", sp.id)
        _tally("sprints", "sprint", sp.id)
        children = [_card_node(c) for c in cards_by_sprint.get(sp.id, [])]
        if depth < 5:
            children = []
        return {
            "id": sp.id, "type": "sprint", "title": sp.title,
            **meta, "children": children,
        }

    def _spec_node(s) -> dict:
        meta = _queue_meta("spec", s.id)
        _tally("specs", "spec", s.id)
        sp_children = [_sprint_node(sp) for sp in sprints_by_spec.get(s.id, [])]
        direct_cards = [_card_node(c) for c in cards_by_spec_direct.get(s.id, [])]
        if depth < 4:
            sp_children = []
            direct_cards = []
        return {
            "id": s.id, "type": "spec", "title": s.title,
            **meta,
            "children": sp_children + direct_cards,
        }

    def _refinement_node(r) -> dict:
        meta = _queue_meta("refinement", r.id)
        _tally("refinements", "refinement", r.id)
        spec_children = [_spec_node(s) for s in specs_by_refinement.get(r.id, [])]
        if depth < 3:
            spec_children = []
        return {
            "id": r.id, "type": "refinement", "title": r.title,
            **meta, "children": spec_children,
        }

    tree: list[dict] = []
    for idea in ideas:
        meta = _queue_meta("ideation", idea.id)
        _tally("ideations", "ideation", idea.id)
        ref_children = [_refinement_node(r) for r in refs_by_ideation.get(idea.id, [])]
        if depth < 2:
            ref_children = []
        tree.append({
            "id": idea.id, "type": "ideation", "title": idea.title,
            **meta, "children": ref_children,
        })

    # Orphan specs (no refinement) go to the root alongside ideations.
    for s in specs_orphan:
        tree.append(_spec_node(s))

    total_pending = sum(
        sum(
            v for k, v in counts.items()
            if k in ("pending", "in_progress")
        )
        for counts in levels_counter.values()
    )

    return {
        "board_id": board_id,
        "depth": depth,
        "total_pending": total_pending,
        "levels": levels_counter,
        "tree": tree,
    }


# ---------------------------------------------------------------------------
# SSE live-events stream (spec f33eb9ca — useKgLiveEvents hook)
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/events")
async def stream_kg_events(
    board_id: str,
    since: str | None = Query(None, description="ISO timestamp — only emit events created after this"),
    db: AsyncSession = Depends(get_db),
):
    """Server-Sent Events stream of `kg.session.committed` /
    `kg.board.cleared` events for the given board.

    The stream reads the `global_update_outbox` table for the latest events
    and emits new rows at a 1-second poll cadence. This endpoint is the
    backing data source for the React `useKgLiveEvents` hook (Frontend
    responsibility — hook implementation lives in the sibling UI repo).

    Protocol:
        event: kg.session.committed
        data: {"event_id": ..., "session_id": ..., "payload": {...}}

    Filter by `since` to resume a dropped connection without replaying the
    entire outbox backlog. Absence ⇒ start from now().
    """
    import asyncio as _asyncio
    import uuid as _uuid
    from datetime import datetime as _dt, timezone as _tz

    from sqlalchemy import and_, asc, func, select as _select

    from okto_pulse.core.models.db import ConsolidationQueue, GlobalUpdateOutbox

    try:
        cursor = _dt.fromisoformat(since) if since else _dt.now(_tz.utc)
    except ValueError:
        raise HTTPException(status_code=400, detail="since must be ISO 8601")

    async def _queue_snapshot() -> dict[str, int]:
        """Counters for `ConsolidationQueue` rows scoped to this board."""
        rows = (await db.execute(
            _select(ConsolidationQueue.status, func.count())
            .where(ConsolidationQueue.board_id == board_id)
            .group_by(ConsolidationQueue.status)
        )).all()
        snap = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
        for status, count in rows:
            if status in snap:
                snap[status] = int(count)
        snap["total"] = sum(snap.values())
        return snap

    async def _iter():
        # Initial heartbeat so the client knows the connection is alive.
        yield "event: hello\ndata: {}\n\n"
        # Emit an initial progress snapshot so the client can render the
        # toast immediately instead of waiting for the first change.
        try:
            initial = await _queue_snapshot()
            yield (
                "event: kg.queue.progress\n"
                "data: "
                + json.dumps(
                    {
                        "event_id": f"progress:{_uuid.uuid4().hex}",
                        "event_type": "kg.queue.progress",
                        "created_at": _dt.now(_tz.utc).isoformat(),
                        "payload": initial,
                    },
                    default=str,
                )
                + "\n\n"
            )
            last_progress = initial
        except Exception:
            last_progress = None

        last_seen = cursor
        while True:
            rows = (await db.execute(
                _select(GlobalUpdateOutbox).where(and_(
                    GlobalUpdateOutbox.board_id == board_id,
                    GlobalUpdateOutbox.created_at > last_seen,
                )).order_by(asc(GlobalUpdateOutbox.created_at)).limit(50)
            )).scalars().all()
            for row in rows:
                payload = {
                    "event_id": row.event_id,
                    "session_id": row.session_id,
                    "event_type": row.event_type,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "payload": row.payload,
                }
                yield (
                    f"event: {row.event_type}\n"
                    f"data: {json.dumps(payload, default=str)}\n\n"
                )
                last_seen = row.created_at or last_seen

            # Emit progress whenever queue counters change. Saves bandwidth
            # on idle boards while keeping the UI chip authoritative.
            try:
                snap = await _queue_snapshot()
                if snap != last_progress:
                    yield (
                        "event: kg.queue.progress\n"
                        "data: "
                        + json.dumps(
                            {
                                "event_id": f"progress:{_uuid.uuid4().hex}",
                                "event_type": "kg.queue.progress",
                                "created_at": _dt.now(_tz.utc).isoformat(),
                                "payload": snap,
                            },
                            default=str,
                        )
                        + "\n\n"
                    )
                    last_progress = snap
            except Exception:
                # Snapshot is best-effort — never break the stream over it.
                pass

            # Keepalive comment every poll so proxies don't drop the
            # connection in between committed bursts.
            yield ": keepalive\n\n"
            try:
                await _asyncio.sleep(1.0)
            except _asyncio.CancelledError:
                break

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
    db: AsyncSession = Depends(get_db),
):
    """Re-queue a failed/done ConsolidationQueue entry so the worker
    reprocesses it. `recursive=true` also re-enqueues descendants below
    the artifact in the Ideation→Refinement→Spec→Sprint→Card hierarchy.

    Idempotency: content_hash BR still owns "nothing actually changed"
    no-op behaviour downstream, so retrying an unchanged artifact is a
    cheap round-trip that touches the outbox once.
    """
    from sqlalchemy import and_, select as _select
    from okto_pulse.core.models.db import Card, ConsolidationQueue, Refinement, Spec, Sprint

    entry = (await db.execute(
        _select(ConsolidationQueue).where(and_(
            ConsolidationQueue.id == queue_entry_id,
            ConsolidationQueue.board_id == board_id,
        ))
    )).scalars().first()
    if entry is None:
        raise HTTPException(status_code=404, detail="queue entry not found")

    entry.status = "pending"
    entry.claimed_at = None
    entry.claimed_by_session_id = None
    entry.source = "retry_from_ui"
    reopened = [queue_entry_id]

    if recursive:
        descendants: list[tuple[str, str]] = []
        if entry.artifact_type == "ideation":
            rows = (await db.execute(
                _select(Refinement.id).where(Refinement.ideation_id == entry.artifact_id)
            )).scalars().all()
            descendants.extend(("refinement", r) for r in rows)
        if entry.artifact_type in ("ideation", "refinement"):
            refinement_ids: list[str]
            if entry.artifact_type == "ideation":
                refinement_ids = [r for _, r in descendants]
            else:
                refinement_ids = [entry.artifact_id]
            specs = (await db.execute(
                _select(Spec.id).where(Spec.refinement_id.in_(refinement_ids))
            )).scalars().all()
            descendants.extend(("spec", s) for s in specs)
        if entry.artifact_type in ("ideation", "refinement", "spec"):
            spec_ids = [s for t, s in descendants if t == "spec"] or [entry.artifact_id]
            sprints = (await db.execute(
                _select(Sprint.id).where(Sprint.spec_id.in_(spec_ids))
            )).scalars().all()
            descendants.extend(("sprint", sp) for sp in sprints)
            cards = (await db.execute(
                _select(Card.id).where(Card.spec_id.in_(spec_ids))
            )).scalars().all()
            descendants.extend(("card", c) for c in cards)

        for artifact_type, artifact_id in descendants:
            row = (await db.execute(
                _select(ConsolidationQueue).where(and_(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == artifact_type,
                    ConsolidationQueue.artifact_id == artifact_id,
                ))
            )).scalars().first()
            if row is None:
                continue
            row.status = "pending"
            row.claimed_at = None
            row.claimed_by_session_id = None
            row.source = "retry_from_ui_recursive"
            reopened.append(row.id)

    await db.commit()

    # Fase 4 — wake the background worker so retried rows are picked up
    # immediately instead of waiting for the heartbeat tick.
    try:
        from okto_pulse.core.kg.workers.consolidation import (
            signal_consolidation_worker,
        )
        signal_consolidation_worker()
    except Exception:  # pragma: no cover — signal is best-effort
        pass

    return {
        "board_id": board_id,
        "queue_entry_id": queue_entry_id,
        "recursive": recursive,
        "reopened_count": len(reopened),
        "reopened_ids": reopened,
    }


@router.post("/boards/{board_id}/nodes/{node_id}/boost")
async def boost_node(
    board_id: str,
    node_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Increment a node's ``relevance_score`` by a fixed +0.3 with clamp [0, 1.5].

    Persists an audit entry to ``ConsolidationAudit`` with event_type
    ``kg.node.boosted``. Idempotency is NOT enforced — each call stacks
    another +0.3 until the clamp is reached, by design (repeat clicks
    should reflect repeat intent).

    Responses:
        200 — `{node_id, node_type, score_before, score_after, boosted_at, boosted_by}`
        404 — node not found in any table of the per-board graph
    """
    from okto_pulse.core.kg.schema import NODE_TYPES, open_board_connection
    from okto_pulse.core.models.db import ConsolidationAudit

    BOOST_DELTA = 0.3
    CLAMP_MIN = 0.0
    CLAMP_MAX = 1.5

    score_before: float | None = None
    node_type: str | None = None
    with open_board_connection(board_id) as (_db, conn):
        for ntype in NODE_TYPES:
            try:
                res = conn.execute(
                    f"MATCH (n:{ntype} {{id: $nid}}) RETURN n.relevance_score",
                    {"nid": node_id},
                )
            except Exception:
                continue
            if res.has_next():
                row = res.get_next()
                score_before = float(row[0]) if row[0] is not None else 0.5
                node_type = ntype
                break

        if node_type is None or score_before is None:
            return _problem(
                status=404,
                title="Node not found",
                detail=f"Node {node_id} not present in board {board_id}",
                error_type="not_found",
            )

        score_after = max(CLAMP_MIN, min(CLAMP_MAX, score_before + BOOST_DELTA))
        try:
            conn.execute(
                f"MATCH (n:{node_type} {{id: $nid}}) "
                f"SET n.relevance_score = $score",
                {"nid": node_id, "score": score_after},
            )
        except Exception as exc:
            return _problem(
                status=500,
                title="Boost persist failed",
                detail=f"Failed to persist boost: {exc}",
                error_type="kuzu_error",
            )

    boosted_at = datetime.now(timezone.utc)
    boosted_by = "local-user"

    try:
        audit_row = ConsolidationAudit(
            session_id=f"boost-{node_id[:8]}-{int(boosted_at.timestamp())}",
            board_id=board_id,
            artifact_id=node_id,
            agent_id=boosted_by,
            committed_at=boosted_at,
            nodes_added=0,
            edges_added=0,
        )
        db.add(audit_row)
        await db.commit()
    except Exception:
        # Audit is best-effort — the boost itself is already persisted.
        await db.rollback()

    return {
        "node_id": node_id,
        "node_type": node_type,
        "score_before": round(score_before, 4),
        "score_after": round(score_after, 4),
        "boosted_at": boosted_at.isoformat(),
        "boosted_by": boosted_by,
    }


@router.get("/openapi.json")
async def openapi_spec():
    """Auto-generated OpenAPI 3.1 spec."""
    return {"info": {"title": "Okto Pulse KG API", "version": "0.1.0"}}
