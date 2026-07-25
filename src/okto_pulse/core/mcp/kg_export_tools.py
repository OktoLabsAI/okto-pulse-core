"""MCP surface for the logical graph export (spec MKG-E-S1 FR6).

Separate module by design (R4 — server.py is >17k lines; same pattern as
kg_power_tools). Registered via ``register_kg_export_tools(mcp, get_agent)``.
"""

from __future__ import annotations

import json
import logging

from okto_pulse.core.mcp.kg_authorization import (
    kg_permission_error,
    principal_id,
)

logger = logging.getLogger(__name__)


def _err(code: str, message: str, **extra) -> str:
    return json.dumps({"error": code, "message": message, **extra})


def register_kg_export_tools(
    mcp,
    *,
    get_agent,
    get_board_agent=None,
) -> None:
    async def _authorized_board_agent(board_id: str):
        agent = await get_agent()
        if agent is None or get_board_agent is None:
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        try:
            board_agent = await get_board_agent(board_id)
        except Exception:
            logger.warning(
                "kg.export.board_acl_resolution_failed board=%s",
                board_id,
                exc_info=True,
            )
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        if board_agent is None:
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        if principal_id(board_agent) != principal_id(agent):
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        permission_error = kg_permission_error(board_agent, "board.read")
        if permission_error is not None:
            return None, _err(
                "permission_denied",
                permission_error,
                required_permission="board.read",
            )
        return agent, None

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

        _agent, auth_error = await _authorized_board_agent(board_id)
        if auth_error is not None:
            return auth_error
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
