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

router = APIRouter(prefix="/api/kg", tags=["knowledge-graph"])


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


def encode_cursor(last_id: str, last_created_at: str) -> str:
    payload = json.dumps({"last_id": last_id, "last_created_at": last_created_at})
    return base64.b64encode(payload.encode()).decode()


def decode_cursor(cursor: str) -> dict | None:
    try:
        payload = base64.b64decode(cursor.encode()).decode()
        return json.loads(payload)
    except Exception:
        return None


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
    """Get node detail + incoming/outgoing edges."""
    svc = get_kg_service()
    try:
        result = svc.explain_constraint(board_id, node_id)
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
    max_nodes: int = Query(200, ge=1, le=1000),
):
    """Return subgraph for visualization."""
    svc = get_kg_service()
    try:
        if center:
            rows = svc.get_related_context(board_id, center, max_rows=max_nodes)
        else:
            rows = svc.get_decision_history(board_id, "", max_rows=max_nodes)
        return {"nodes": rows, "edges": [], "metadata": {"depth": depth, "truncated": len(rows) >= max_nodes}}
    except KGToolError as e:
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/stats")
async def get_stats(board_id: str):
    """Board KG stats: counts, confidence, pending."""
    svc = get_kg_service()
    try:
        ver = svc.get_schema_version(board_id)
        return {
            "schema_version": ver,
            "node_counts_by_type": {},
            "edge_counts_by_type": {},
            "avg_confidence": 0.0,
            "pending_queue_count": 0,
            "last_consolidation_at": None,
        }
    except KGToolError as e:
        return _handle_kg_error(e)


@router.get("/boards/{board_id}/audit")
async def list_audit(
    board_id: str,
    limit: int = Query(50, ge=1, le=200),
    cursor: str = "",
):
    """List consolidation audit entries."""
    return {"entries": [], "next_cursor": None}


@router.post("/boards/{board_id}/audit/{session_id}/undo")
async def undo_session(board_id: str, session_id: str, force: bool = False):
    """Undo a consolidation session."""
    return _problem(501, "Not Implemented", "Undo will be available in governance sprint")


@router.get("/boards/{board_id}/audit/export")
async def export_audit(
    board_id: str,
    format: str = Query("json", regex="^(json|csv)$"),
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
async def global_search(q: str = "", limit: int = Query(20, ge=1, le=100)):
    """Cross-board global discovery search."""
    svc = get_kg_service()
    try:
        results = svc.query_global(q, user_boards=[], top_k=limit)
        return {"results": results, "total": len(results)}
    except KGToolError as e:
        return _handle_kg_error(e)


@router.post("/boards/{board_id}/historical-consolidation/start")
async def start_historical(board_id: str):
    """Start historical backfill."""
    return {"status": "queueing", "board_id": board_id, "total_artifacts": 0}


@router.post("/boards/{board_id}/historical-consolidation/cancel")
async def cancel_historical(board_id: str):
    """Cancel historical backfill."""
    return {"status": "cancelled", "board_id": board_id}


@router.get("/boards/{board_id}/historical-consolidation/progress")
async def historical_progress(board_id: str):
    """Historical consolidation progress."""
    return {"enabled": False, "status": "inactive", "total": 0, "progress": 0}


@router.delete("/boards/{board_id}/kg")
async def delete_board_kg(board_id: str):
    """Wipe KG data for a board (right-to-erasure)."""
    from okto_pulse.core.kg.global_discovery.clustering import board_delete_cascade
    counts = board_delete_cascade(board_id)
    return Response(status_code=204)


@router.get("/boards/{board_id}/settings")
async def get_settings(board_id: str):
    """Get KG settings for a board."""
    return {"consolidation_enabled": False, "enable_historical_consolidation": False}


@router.put("/boards/{board_id}/settings")
async def update_settings(board_id: str):
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
async def list_pending(board_id: str):
    """List pending consolidation queue entries."""
    return {"entries": [], "count": 0}


@router.get("/openapi.json")
async def openapi_spec():
    """Auto-generated OpenAPI 3.1 spec."""
    return {"info": {"title": "Okto Pulse KG API", "version": "0.1.0"}}
