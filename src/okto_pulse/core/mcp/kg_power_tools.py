"""MCP tools for the 3 tier power escape hatch tools.

Registered via `register_kg_power_tools(mcp, get_agent, get_db)`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    check_rate_limit,
    compute_pattern_hash,
    execute_cypher_read_only,
    execute_natural_query,
    get_schema_info,
)

logger = logging.getLogger(__name__)


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
        logger.debug("[KG] kg_query_cypher called: board_id=%s cypher_len=%d max_rows=%d timeout_ms=%d",
                     board_id, len(cypher), max_rows, timeout_ms)
        try:
            check_rate_limit(agent.id)
            logger.debug("[KG] kg_query_cypher offloading to thread")
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_cypher_read_only,
                    board_id, cypher, params,
                    max_rows=max_rows, timeout_ms=timeout_ms,
                ),
                timeout=30.0,
            )
            logger.debug("[KG] kg_query_cypher thread returned: row_count=%d",
                         result.get("row_count", "unknown"))
            return json.dumps(result, default=str)
        except asyncio.TimeoutError:
            logger.error("[KG] kg_query_cypher timed out after 30s: board_id=%s", board_id)
            return _err("timeout", "Query exceeded 30s timeout")
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
        logger.debug("[KG] kg_query_natural called: board_id=%s query=%r limit=%d",
                     board_id, nl_query[:80], limit)
        try:
            check_rate_limit(agent.id)
            logger.debug("[KG] kg_query_natural offloading to thread")
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_natural_query,
                    board_id, nl_query,
                    limit=limit, min_confidence=min_confidence,
                    since=since or None, until=until or None,
                ),
                timeout=30.0,
            )
            logger.debug("[KG] kg_query_natural thread returned: total_matches=%d",
                         result.get("total_matches", "unknown"))
            return json.dumps(result, default=str)
        except asyncio.TimeoutError:
            logger.error("[KG] kg_query_natural timed out after 30s: board_id=%s", board_id)
            return _err("timeout", "Query exceeded 30s timeout")
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

        logger.debug("[KG] kg_schema_info called: board_id=%s include_internal=%s",
                     board_id, include_internal)
        want_internal = include_internal.lower() in ("true", "1", "yes")
        logger.debug("[KG] kg_schema_info offloading to thread")
        result = await asyncio.wait_for(
            asyncio.to_thread(
                get_schema_info,
                board_id or "default",
                include_internal=want_internal,
            ),
            timeout=30.0,
        )
        logger.debug("[KG] kg_schema_info thread returned: schema_version=%s",
                     result.get("schema_version", "unknown"))
        return json.dumps(result, default=str)

    @mcp.tool()
    async def okto_pulse_kg_verify_grounding(
        board_id: str,
        answer_text: str,
        retrieved_rows_json: str,
        pre_extracted_entities_json: str = "",
    ) -> str:
        """
        Verify that an agent answer is grounded in the retrieved KG nodes.

        Deterministic entity check only in this V1 — matches entity names
        against retrieved row titles via normalized exact match (NFKD +
        strip diacritics + lowercase) with Jaccard fallback (threshold
        0.7). Semantic grounding via LLM is available programmatically
        via the Python API `verify_grounding(..., extractor_fn=,
        grounder_fn=)` but not exposed over MCP (no LLM wired here).

        Ideação d3dfdab8. Enforcement is decoupled — this tool returns
        the verdict; the caller (agent, UI, critic loop) decides what to
        do with it.

        Args:
            board_id: Board ID for authorization (kg.query.global).
            answer_text: The agent's response to verify.
            retrieved_rows_json: JSON string — list of
                `{"node_id": ..., "title": ..., ...}` rows the answer
                was based on.
            pre_extracted_entities_json: Optional JSON array of strings
                listing the entity names the caller wants to check. If
                empty, falls back to heuristic extraction (quoted terms
                and capitalised multi-word phrases).

        Returns:
            JSON with the GroundingResult fields: overall_grounded,
            confidence, hallucinated_entities, unsupported_claims,
            attribution_map.

        Raises:
            ValueError: if retrieved_rows_json is not valid JSON.
        """
        import re

        from okto_pulse.core.kg.grounding import check_entities_present

        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")

        try:
            rows = json.loads(retrieved_rows_json) if retrieved_rows_json else []
        except json.JSONDecodeError as e:
            raise ValueError(
                f"retrieved_rows_json is not valid JSON: {e}"
            ) from e
        if not isinstance(rows, list):
            raise ValueError("retrieved_rows_json must decode to a list")

        # Entity source: explicit list, or heuristic from answer_text.
        entities: list[str] = []
        if pre_extracted_entities_json:
            try:
                raw = json.loads(pre_extracted_entities_json)
                if isinstance(raw, list):
                    entities = [str(e) for e in raw if e]
            except json.JSONDecodeError:
                # Invalid entities JSON is non-fatal — fall back to heuristic.
                pass
        if not entities:
            # Heuristic: quoted terms + capitalised 2+ word phrases.
            entities = re.findall(r'"([^"]{2,80})"', answer_text)
            entities += re.findall(
                r"\b(?:[A-Z][\w-]+(?:\s+[A-Z][\w-]+){1,4})\b",
                answer_text,
            )
            # Dedupe preserving order.
            seen: set[str] = set()
            entities = [e for e in entities if not (e in seen or seen.add(e))]

        present, hallucinated = check_entities_present(entities, rows)

        overall_grounded = not hallucinated
        confidence = 1.0 if overall_grounded else 0.0

        result = {
            "overall_grounded": overall_grounded,
            "confidence": confidence,
            "hallucinated_entities": sorted(hallucinated),
            "unsupported_claims": [],
            "attribution_map": [],
            "note": (
                "MCP V1 does deterministic entity-check only. "
                "For full semantic grounding use the Python API with "
                "an LLM callable (see okto_pulse.core.kg.grounding)."
            ),
        }
        return json.dumps(result, default=str)

    @mcp.tool()
    async def okto_pulse_kg_query_reflective(
        board_id: str,
        nl_query: str,
        limit: int = 20,
    ) -> str:
        """
        V1 stub of the reflective retrieve loop (ideação db8e984f).

        The full agentic loop (critic_evaluate → dispatch action →
        retrieve retry) requires an LLM callable (critic_fn) — MCP
        tools can't receive Python callables, so this V1 delegates to
        the standard execute_natural_query and labels the response
        as a "v1_stub_no_critic_wired" stop reason.

        To use the real loop, call
        ``okto_pulse.core.kg.retrieve_critic.reflect()`` programmatically
        from a Python host that wires its own LLM provider.

        Args:
            board_id: Board ID (authorization: kg.query.global).
            nl_query: Natural-language query (same as
                okto_pulse_kg_query_natural).
            limit: Max rows (default 20).

        Returns:
            JSON with rows + reflection metadata:
            ``{nodes, total_matches, stopped_reason, iterations}``.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            check_rate_limit(agent.id)
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_natural_query,
                    board_id, nl_query, limit=limit,
                ),
                timeout=30.0,
            )
            payload = {
                "nodes": result.get("nodes", []),
                "total_matches": result.get("total_matches", 0),
                "stopped_reason": "v1_stub_no_critic_wired",
                "iterations": [
                    {
                        "iteration": 0,
                        "adequacy": "sufficient",
                        "action": "accept",
                        "rows_count": len(result.get("nodes", [])),
                        "note": (
                            "V1 stub: no critic LLM wired over MCP. "
                            "Use reflect() in Python for the full loop."
                        ),
                    }
                ],
            }
            return json.dumps(payload, default=str)
        except asyncio.TimeoutError:
            return _err("timeout", "Query exceeded 30s timeout")
        except TierPowerError as e:
            return _err(e.code, e.message, details=e.details)
