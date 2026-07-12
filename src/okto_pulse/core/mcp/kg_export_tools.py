"""MCP surface for the logical graph export (spec MKG-E-S1 FR6).

Separate module by design (R4 — server.py is >17k lines; same pattern as
kg_power_tools). Registered via ``register_kg_export_tools(mcp, get_agent)``.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def _err(code: str, message: str) -> str:
    return json.dumps({"error": code, "message": message})


def register_kg_export_tools(mcp, *, get_agent) -> None:
    @mcp.tool()
    async def okto_pulse_kg_export_jsonld(
        board_id: str,
        cursor: str = "",
        page_size: int = 200,
    ) -> str:
        """Read-only JSON-LD export of a board graph with a fixed PROV-O
mapping (prov:Entity, wasDerivedFrom, wasGeneratedBy, wasAttributedTo,
wasRevisionOf; pulse:nodeType/kindOf; typed pulse: edges). Paged by a stable
node_id cursor — pass next_cursor until last_page=true; page concatenation is
the full deterministic export. Unreadable graph → kg_export_failed, never a
partial document. The graph is never modified."""
        from okto_pulse.core.kg.graph_export import (
            GraphExportError,
            export_board_jsonld,
        )

        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        import asyncio

        try:
            document = await asyncio.wait_for(
                asyncio.to_thread(
                    export_board_jsonld,
                    board_id,
                    cursor=cursor or None,
                    page_size=max(1, min(int(page_size), 1000)),
                ),
                timeout=60.0,
            )
        except GraphExportError as exc:
            return _err(exc.code, str(exc))
        return json.dumps(document, sort_keys=True, default=str)
