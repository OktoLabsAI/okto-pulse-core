"""MCP tools for the 3 tier power escape hatch tools.

Registered via `register_kg_power_tools(mcp, get_agent, get_db)`.
"""

from __future__ import annotations

import json
from typing import Any

from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    check_rate_limit,
    compute_pattern_hash,
    execute_cypher_read_only,
    execute_natural_query,
    get_schema_info,
)


def _err(code: str, message: str, **extra: Any) -> str:
    payload: dict = {"error": {"code": code, "message": message}}
    if extra:
        payload["error"].update(extra)
    return json.dumps(payload, default=str)


def register_kg_power_tools(mcp, *, get_agent, get_db) -> None:

    @mcp.tool()
    async def okto_pulse_kg_query_cypher(
        board_id: str,
        cypher: str,
        params: dict | None = None,
        max_rows: int = 1000,
        timeout_ms: int = 5000,
    ) -> str:
        """
        Execute a read-only Cypher query directly against a board's Kuzu graph.

        Safety rails applied automatically:
        - Parser whitelist rejects write keywords (CREATE/DELETE/SET/etc)
        - Comment stripping + unicode normalization
        - Auto-inject LIMIT if missing
        - Variable-length paths auto-bounded to *..20
        - Timeout 5s default, 30s max
        - Rate limit 30 queries/min per agent

        Args:
            board_id: Board ID
            cypher: Read-only Cypher query string
            params: Optional parameter dict for parameterized queries
            max_rows: Max rows (default 1000, max 10000)
            timeout_ms: Timeout in ms (default 5000, max 30000)

        Returns:
            JSON with rows, row_count, truncated, execution_time_ms
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            check_rate_limit(agent.id)
            result = execute_cypher_read_only(
                board_id, cypher, params,
                max_rows=max_rows, timeout_ms=timeout_ms,
            )
            return json.dumps(result, default=str)
        except TierPowerError as e:
            return _err(e.code, e.message, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_query_natural(
        board_id: str,
        nl_query: str,
        limit: int = 20,
        min_confidence: float = 0.5,
        since: str = "",
        until: str = "",
    ) -> str:
        """
        Natural language search over the board's knowledge graph. Uses hybrid
        search (embedding + HNSW + traversal). Falls back to string match if
        embedding is unavailable.

        Does NOT invoke any LLM — all processing is deterministic (embedding
        model is local sentence-transformers or stub).

        Args:
            board_id: Board ID
            nl_query: Natural language query
            limit: Max results (default 20)
            min_confidence: Min confidence threshold (default 0.5)
            since: Optional ISO-8601 timestamp — return only nodes with
                ``created_at >= since``. Empty string = no lower bound.
                Invalid timestamps are ignored (best-effort).
            until: Optional ISO-8601 timestamp — return only nodes with
                ``created_at <= until``. Empty string = no upper bound.

        Returns:
            JSON with nodes, total_matches, optional warning. When a temporal
            filter is active the response also carries ``temporal_filter``
            metadata (candidates_before_filter, filtered_out).
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            check_rate_limit(agent.id)
            result = execute_natural_query(
                board_id, nl_query,
                limit=limit, min_confidence=min_confidence,
                since=since or None, until=until or None,
            )
            return json.dumps(result, default=str)
        except TierPowerError as e:
            return _err(e.code, e.message, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_schema_info(
        board_id: str = "",
        include_internal: str = "false",
    ) -> str:
        """
        Return schema introspection: stable node types, rel types, vector
        indexes. Internal types require include_internal=true + admin role.

        Args:
            board_id: Optional board ID (empty = global schema namespace)
            include_internal: "true" to include internal types (admin only)

        Returns:
            JSON with schema_version, stable_node_types, stable_rel_types,
            vector_indexes, optionally internal_*_types
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")

        want_internal = include_internal.lower() in ("true", "1", "yes")
        result = get_schema_info(
            board_id or "default",
            include_internal=want_internal,
        )
        return json.dumps(result, default=str)
