"""KG dashboard readers backed by the KG operational read-model port."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.ports.kg_operational import (
    get_kg_operational_read_model_port,
)


async def list_consolidation_audit(
    context: Any,
    board_id: str,
    *,
    limit: int,
    include_code_traceability: bool = True,
) -> list[dict[str, Any]]:
    """Return committed consolidation-audit entries for a board, newest first."""
    reader = get_kg_operational_read_model_port()
    rows = (
        await reader.list_consolidation_audit(
            context,
            board_id=board_id,
            limit=limit,
        )
        if include_code_traceability
        else await reader.list_consolidation_audit(
            context,
            board_id=board_id,
            limit=limit,
            include_code_traceability=False,
        )
    )
    return [dict(row) for row in rows]


async def list_all_board_ids(context: Any, *, limit: int = 100) -> list[str]:
    """Return the ids of all boards visible to the edition adapter."""
    rows = await get_kg_operational_read_model_port().list_all_board_ids(
        context,
        limit=limit,
    )
    return [str(row) for row in rows]


# ---------------------------------------------------------------------------
# Pending queue readers (spec R01A REST-FU5-S3 — list_pending / list_pending_tree)
# ---------------------------------------------------------------------------


async def list_pending_entries(
    context: Any,
    board_id: str,
    *,
    include_code_traceability: bool = True,
) -> list[dict[str, Any]]:
    """Return the board's pending consolidation-queue entries, newest first."""
    reader = get_kg_operational_read_model_port()
    rows = (
        await reader.list_pending_entries(
            context,
            board_id=board_id,
        )
        if include_code_traceability
        else await reader.list_pending_entries(
            context,
            board_id=board_id,
            include_code_traceability=False,
        )
    )
    return [dict(row) for row in rows]


async def build_pending_tree(
    context: Any, board_id: str, *, depth: int = 5
) -> dict[str, Any]:
    """Build the hierarchical pending-queue view."""
    payload = await get_kg_operational_read_model_port().build_pending_tree(
        context,
        board_id=board_id,
        depth=depth,
    )
    return dict(payload)
