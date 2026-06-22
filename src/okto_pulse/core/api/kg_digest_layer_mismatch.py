"""R1-IMP2 — REST drilldown for digest_vs_board_layer_mismatch.

Read-only. KG Health surfaces ``digest_vs_board_layer_mismatch`` as an aggregate
``health_issues[]`` entry pointing here via ``drill_down_tool``; the per-digest
rows (published vs expected publication layer) live in this drilldown. Mirrored
1:1 by the MCP tool ``okto_pulse_kg_digest_layer_mismatch_list``. No mutation /
remediation — the R1-IMP1 reconciler corrects mismatches on the next drain.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.global_discovery.layer_parity import (
    list_digest_layer_mismatches,
)

router = APIRouter()


@router.get(
    "/kg/{board_id}/digest-layer-mismatch",
    tags=["kg-digest-layer-mismatch"],
)
async def list_digest_layer_mismatch_endpoint(
    board_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Read-only list of digest-vs-board publication-layer mismatches.

    Each item: board_id, digest_id, original_node_id, node_type, expected_layer,
    actual_layer, source_artifact_ref. Degrades to an empty list if the global or
    board graph is unreadable (never 5xx for a transient storage state).
    """
    return await list_digest_layer_mismatches(
        db, board_id=board_id, limit=limit, offset=offset,
    )
