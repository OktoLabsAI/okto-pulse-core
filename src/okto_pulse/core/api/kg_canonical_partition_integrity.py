"""R7 IMP4 — REST drilldown for canonical Learning partition integrity.

Read-only. KG Health surfaces ``canonical_partition_integrity`` as an AGGREGATE
``health_issues[]`` entry that points here via ``drill_down_tool``; the per-node
rows (go-forward holds, historical debt, mixed-evidence deferred, provenance-only
observed) live ONLY in this drilldown. Mirrored 1:1 by the MCP tool
``okto_pulse_kg_canonical_partition_integrity_list``. No skip/retry/mutation.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.canonical_partition_integrity import (
    get_canonical_partition_integrity_detail,
    list_canonical_partition_integrity,
)
from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError

router = APIRouter()


@router.get(
    "/kg/{board_id}/canonical-partition-integrity",
    tags=["kg-canonical-partition-integrity"],
)
async def list_canonical_partition_integrity_endpoint(
    board_id: str,
    reason_code: str | None = Query(None),
    graph_layer: str | None = Query(None),
    source_ref: str | None = Query(None),
    node_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Read-only list of canonical-partition-integrity signals.

    Each item carries an S-KG-02 ``classification`` (Learning-centric verdict;
    the response also includes a ``classification_counts`` census) alongside the
    preserved ``status`` / ``counts``. Mirrored 1:1 by the MCP tool.

    Errors: 400 ``invalid_filter`` (bad enum filter) / 503 ``kg_health_unavailable``.
    """
    try:
        return await list_canonical_partition_integrity(
            db,
            board_id=board_id,
            reason_code=reason_code,
            graph_layer=graph_layer,
            source_ref=source_ref,
            node_id=node_id,
            status=status,
            limit=limit,
            offset=offset,
        )
    except CognitiveReadinessError as exc:
        raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc


@router.get(
    "/kg/{board_id}/canonical-partition-integrity/{node_id}",
    tags=["kg-canonical-partition-integrity"],
)
async def get_canonical_partition_integrity_detail_endpoint(
    board_id: str,
    node_id: str,
    _user: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Read-only detail for one canonical Learning node.

    Carries the S-KG-02 ``classification`` plus ``canonical_edges`` /
    ``working_edges`` (validates -> Bug) and ``relates_to_edges`` (the taxonomy
    associations), consistent with the list/MCP surface.

    Error: 404 ``canonical_partition_item_not_found``.
    """
    try:
        return await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id
        )
    except CognitiveReadinessError as exc:
        raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc
