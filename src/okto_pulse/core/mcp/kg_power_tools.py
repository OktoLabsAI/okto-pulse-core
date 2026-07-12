"""MCP tools for the 3 tier power escape hatch tools.

Registered via `register_kg_power_tools(mcp, get_agent)`. These graph-only escape
hatches never open a relational session (spec R01A MCP-FU4 dropped the unused
``get_db`` injection).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    check_rate_limit,
    execute_cypher_read_only,
    execute_natural_query,
    get_schema_info,
)
from okto_pulse.core.mcp.kg_query_safety import (
    KGCypherResponseSanitizer,
    KGNaturalQueryBoundaryGuard,
    annotate_truncation,
    resolve_cypher_max_rows,
    round_kg_numbers,
)

logger = logging.getLogger(__name__)


# spec 28583299 (Ideação #4, IMPL-E, dec_bd607339): hit-counting parity for
# kg_query_cypher. Strict v1 extraction (E-1.a in KE-E): only counts a hit
# when the result row carries a recognisable id column or a UUID-like value.
# Agents that want credit for hits MUST shape their RETURN accordingly —
# documented in agent_instructions.md.
_UUID_LIKE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _is_uuid_like(value: Any) -> bool:
    """True when ``value`` matches the canonical 36-char UUID layout."""
    return isinstance(value, str) and bool(_UUID_LIKE.match(value))


def _extract_node_ids_from_cypher_result(
    result: dict,
) -> list[tuple[str, str]]:
    """Return ``[(node_type, node_id), ...]`` pairs found in ``result``.

    Heuristic v1 (strict — see KE-E E-1.a):
    1. Look up ``id``/``node_id`` columns by name (case-insensitive).
    2. Look up ``labels(n)``/``node_type`` columns for the node type.
    3. When neither match, scan each row's scalars for a UUID-like value
       and pair it with ``node_type='unknown'`` so record_query_hit can
       still flush the counter (downstream short-circuits unknown).

    Pure function — no I/O. Returns an empty list when ``result`` lacks
    ``rows``/``columns`` keys (which is the shape execute_cypher_read_only
    consistently returns).
    """
    columns = result.get("columns") or []
    rows = result.get("rows") or []
    if not columns or not rows:
        return []

    id_aliases = {"id", "node_id"}
    type_aliases = {"node_type"}

    id_idx: int | None = None
    type_idx: int | None = None
    for i, col in enumerate(columns):
        col_lower = str(col).lower()
        if id_idx is None and (
            col_lower in id_aliases or col_lower.endswith(".id")
        ):
            id_idx = i
        if type_idx is None and (
            col_lower in type_aliases
            or "label" in col_lower
            or col_lower.endswith(".labels")
        ):
            type_idx = i

    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if not isinstance(row, (list, tuple)):
            continue
        node_id: str | None = None
        node_type: str = "unknown"

        if id_idx is not None and id_idx < len(row):
            value = row[id_idx]
            if _is_uuid_like(value):
                node_id = value

        if type_idx is not None and type_idx < len(row):
            raw = row[type_idx]
            if isinstance(raw, (list, tuple)) and raw:
                node_type = str(raw[0])
            elif isinstance(raw, str):
                node_type = raw.strip("[]'\"")

        if node_id is None:
            for cell in row:
                if _is_uuid_like(cell):
                    node_id = cell
                    break

        if node_id is None:
            continue
        key = (node_type, node_id)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)
    return pairs


async def _record_cypher_hits(
    board_id: str, pairs: list[tuple[str, str]],
) -> None:
    """Fire-and-forget loop that increments the hit counter for each pair.

    ``record_query_hit`` is a thin wrapper around the in-process counter
    that lazy-flushes to the graph store when ``HIT_FLUSH_THRESHOLD`` is reached. The
    invocation must NEVER block the query response (BR3 + dec_3a6eb8ad).
    """
    try:
        from okto_pulse.core.kg.kg_service import KGService

        service = KGService()
        for node_type, node_id in pairs:
            try:
                await service.increment_hit(board_id, node_type, node_id)
            except Exception as exc:
                logger.debug(
                    "kg.cypher.hit_skipped board=%s node=%s err=%s",
                    board_id, node_id, exc,
                )
    except Exception as exc:
        logger.debug(
            "kg.cypher.hit_record_failed board=%s err=%s", board_id, exc,
        )


def _err(code: str, message: str, **extra: Any) -> str:
    payload: dict = {"error": {"code": code, "message": message}}
    if extra:
        payload["error"].update(extra)
    return json.dumps(payload, default=str)


def register_kg_power_tools(mcp, *, get_agent) -> None:

    @mcp.tool()
    async def okto_pulse_kg_query_cypher(
        board_id: str,
        cypher: str,
        params: dict | None = None,
        max_rows: int = 0,
        timeout_ms: int = 5000,
        include_working: bool = False,
    ) -> str:
        """Execute a read-only Cypher query against a board's graph. Safety rails auto-applied:
write-keyword whitelist (CREATE/DELETE/SET rejected), comment strip + unicode
normalize, auto-LIMIT, variable-length paths bounded to *..20, timeout 5s default /
30s max, rate limit 30/min, embedding/vector columns STRIPPED from the response,
rows bounded to an agent-safe page, numeric scores rounded. max_rows: 0=agent-safe
default (50), 1..1000 explicit, >1000 rejected. Full args/returns:
okto-pulse://reference/tool-docs/kg."""
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_query_cypher called: board_id=%s cypher_len=%d max_rows=%d timeout_ms=%d",
                     board_id, len(cypher), max_rows, timeout_ms)
        try:
            check_rate_limit(agent.id)
            # FR2/FR9: clamp to an agent-safe page; reject above the hard cap.
            effective_rows, bound_err = resolve_cypher_max_rows(max_rows)
            if bound_err is not None:
                return _err(
                    bound_err["error"],
                    bound_err["detail"],
                    requested_max_rows=bound_err["requested_max_rows"],
                    hard_cap=bound_err["hard_cap"],
                )
            logger.debug("[KG] kg_query_cypher offloading to thread")
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_cypher_read_only,
                    board_id, cypher, params,
                    max_rows=effective_rows,
                    timeout_ms=timeout_ms,
                    include_working=include_working,
                ),
                timeout=30.0,
            )
            logger.debug("[KG] kg_query_cypher thread returned: row_count=%d",
                         result.get("row_count", "unknown"))

            # spec 28583299 (Ideação #4, IMPL-E, FR18 + dec_bd607339):
            # increment hit counter for each node identified in the result
            # so kg_query_cypher achieves parity with kg_query_natural's
            # ranking signal. Fire-and-forget — never block the query
            # response on the counter side effect.
            try:
                pairs = _extract_node_ids_from_cypher_result(result)
                if pairs:
                    asyncio.create_task(
                        _record_cypher_hits(board_id, pairs)
                    )
            except Exception as exc:
                logger.debug(
                    "kg.cypher.hit_extract_failed board=%s err=%s",
                    board_id, exc,
                )
            # FR1/FR2/FR3: strip embeddings, annotate row bounds, round numeric
            # scores — at the response boundary, AFTER hit-counting (which needs
            # the raw node ids). Ordering of rows is untouched.
            result = KGCypherResponseSanitizer().sanitize(result)
            result = annotate_truncation(result, effective_rows)
            result = round_kg_numbers(result)
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
        graph_layer: str = "canonical",
    ) -> str:
        """Natural-language search over a board's knowledge graph using hybrid
        search (embedding + HNSW + traversal), falling back to string match when
        embedding is unavailable. Deterministic — invokes NO LLM. Optional
        min_confidence (default 0.5) and since/until ISO-8601 bounds on
        created_at; graph_layer (canonical|working|all, default canonical) scopes
        results by KG layer and fails closed on invalid values BEFORE execution.
        Returns nodes, total_matches, applied_graph_layer and a layer_audit where
        metadata/legacy_unknown never count as canonical/working leakage.
        Docs: okto-pulse://reference/tool-docs/kg.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_query_natural called: board_id=%s query=%r limit=%d",
                     board_id, nl_query[:80], limit)
        try:
            check_rate_limit(agent.id)
            # FR0: reject an oversize natural query at the MCP boundary BEFORE
            # the embedding provider is resolved. execute_natural_query resolves
            # registry.require_embedding_provider internally, so the guard MUST run
            # before it is offloaded — never invoking any embedding call.
            rejection = KGNaturalQueryBoundaryGuard().check(nl_query)
            if rejection is not None:
                # Structured query_too_large via the tool's _err envelope; safe
                # metadata only (counts + embedding_invoked), never the query text.
                return _err(
                    "query_too_large",
                    rejection["detail"],
                    embedding_invoked=False,
                    query_chars=rejection["rejection"]["query_chars"],
                    max_chars=rejection["rejection"]["max_chars"],
                )
            logger.debug("[KG] kg_query_natural offloading to thread")
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_natural_query,
                    board_id, nl_query,
                    limit=limit, min_confidence=min_confidence,
                    since=since or None, until=until or None,
                    graph_layer=graph_layer,
                ),
                timeout=30.0,
            )
            logger.debug("[KG] kg_query_natural thread returned: total_matches=%d",
                         result.get("total_matches", "unknown"))
            # FR3: round numeric scores at the response boundary.
            result = round_kg_numbers(result)
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
        """Return KG schema introspection: schema_version, stable node types, rel
        types and vector indexes (global namespace when board_id is empty).
        Internal types require include_internal="true" + admin role.
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
        """Verify an agent answer is grounded in the retrieved KG nodes. V1 is a deterministic
entity check only — normalized exact match (NFKD + strip diacritics + lowercase)
with Jaccard fallback (0.7); no LLM over MCP (semantic grounding is Python-API only).
Returns the verdict (overall_grounded, confidence, hallucinated_entities,
unsupported_claims, attribution_map); enforcement is the caller's decision. Full
args: okto-pulse://reference/tool-docs/kg."""
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
    async def okto_pulse_kg_provenance_drift(
        board_id: str,
        node_type: str = "",
    ) -> str:
        """Read-only artifact→node drift report: compares each node's persisted
source_content_hash against the artifact's latest consolidation and current
existence. Reasons: content_changed | artifact_missing (deleted source, terminal).
Returns checked_count/drifted_count/skipped_count + drifted list. Remedy is a
normal re-consolidation; the graph is never modified. node_type optionally
narrows to one table."""
        from okto_pulse.core.kg.provenance_drift import provenance_drift_report

        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            report = await asyncio.wait_for(
                provenance_drift_report(
                    board_id, node_type or None
                ),
                timeout=60.0,
            )
        except ValueError as e:
            return _err("invalid_node_type", str(e))
        return json.dumps(report, default=str)

    @mcp.tool()
    async def okto_pulse_kg_query_reflective(
        board_id: str,
        nl_query: str,
        limit: int = 20,
    ) -> str:
        """Reflective NL query over a board's KG (authorization: kg.query.global).
        V1 stub: delegates to the kg_query_natural core (params board_id,
        nl_query, limit) and returns nodes, total_matches, iterations and
        stopped_reason='v1_stub_no_critic_wired'.
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
