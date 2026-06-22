"""R2-IMP4 — REST drilldown for stale-canonical parity.

Read-only. KG Health surfaces ``stale_canonical_parity`` as an aggregate
``health_issues[]`` entry pointing here via ``drill_down_tool``; the per-node
rows (canonical deterministic nodes whose source regressed, annotated with the
Global Discovery digest staleness) live in this drilldown. Mirrored 1:1 by the
MCP tool ``okto_pulse_kg_stale_canonical_parity_list``. NO mutation / demotion /
reconcile / Global Discovery sync — diagnostic only.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.stale_canonical_parity import list_stale_canonical_parity

router = APIRouter()


@router.get(
    "/kg/{board_id}/stale-canonical-parity",
    tags=["kg-stale-canonical-parity"],
)
async def list_stale_canonical_parity_endpoint(
    board_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Read-only list of stale-canonical parity signals.

    Each item: node_id, node_type, source_artifact_ref, board_graph_stale,
    global_discovery_stale_digest, expected_graph_layer, expected_maturity_status,
    current_source_status, recommended_action. ``global_discovery_evaluation`` is
    ``not_evaluated`` (never a false healthy) if R1 digest metadata is unreadable.
    """
    return await list_stale_canonical_parity(
        db, board_id=board_id, limit=limit, offset=offset,
    )
