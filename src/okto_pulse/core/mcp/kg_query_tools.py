"""MCP tool wrappers for the 9 tier primario intent-based query tools.

Registered via `register_kg_query_tools(mcp, get_agent, get_db)` called from
server.py. Each tool authenticates, resolves board ACL, delegates to
`kg_service`, and serializes the response as JSON.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from okto_pulse.core.kg.kg_service import (
    KGToolError,
    get_kg_service,
    normalize_graph_layer,
)
from okto_pulse.core.kg.tool_schemas import (
    AlternativeResult,
    AlternativesResponse,
    ConstraintExplanation,
    ConstraintExplanationResponse,
    ContradictionPair,
    ContradictionsResponse,
    ContextHop,
    DecisionHistoryResponse,
    GlobalQueryResponse,
    GlobalResult,
    KGNodeResult,
    LearningResult,
    LearningsResponse,
    RelatedContextResponse,
    SimilarDecisionResult,
    SimilarDecisionsResponse,
    SupersedenceChainResponse,
    SupersedenceEntry,
)

logger = logging.getLogger(__name__)


def _err(code: str, message: str, **extra: Any) -> str:
    payload: dict = {"error": {"code": code, "message": message}}
    if extra:
        payload["error"].update(extra)
    return json.dumps(payload, default=str)


def _err_from(e: KGToolError) -> str:
    """Build the MCP error envelope from a KGToolError, forwarding the degraded
    ``graph_state`` (FR7) into the ``error`` object when the typed error carries
    it (the ``graph_unavailable`` case). Errors without a ``graph_state`` detail
    are emitted unchanged so existing envelopes are unaffected."""
    details = e.details or {}
    if "graph_state" in details:
        return _err(e.code, e.message, graph_state=details["graph_state"])
    return _err(e.code, e.message)


async def _get_auth_context():
    """Get AuthContext from registry or return None."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    factory = get_kg_registry().auth_context_factory
    if factory is None:
        return None
    return factory()


async def _get_user_boards(get_agent=None, get_db=None) -> tuple[Any, list[str]]:
    """Authenticate agent and return (agent_id, board_ids) via AuthContext."""
    auth = await _get_auth_context()
    if auth is not None:
        agent_id = await auth.get_agent_id()
        if agent_id is None:
            return None, []

        class _Stub:
            def __init__(self, id):
                self.id = id

        boards = await auth.get_accessible_boards()
        return _Stub(agent_id), boards

    if get_agent is not None:
        agent = await get_agent()
        if agent is None:
            return None, []
        async with get_db() as db:
            from okto_pulse.core.services.main import AgentService
            svc = AgentService(db)
            boards = await svc.list_boards_for_agent(agent.id)
            await db.commit()
            return agent, [b.id for b in boards]
    return None, []


def register_kg_query_tools(mcp, *, get_agent, get_db) -> None:
    """Register the 9 tier primario query tools on the FastMCP instance."""

    @mcp.tool()
    async def okto_pulse_kg_get_decision_history(
        board_id: str,
        topic: str,
        min_confidence: float = 0.5,
        max_rows: int = 100,
        use_semantic: bool = True,
        min_similarity: float = 0.3,
    ) -> str:
        """Trace decisions about a topic/module over time, with their supersedence chain. topic
accepts natural-language phrases when use_semantic=True (default): embeds the topic
and queries the Decision HNSW index, then backfills with title-CONTAINS. Set
use_semantic=False for deterministic string-only search. Tunables: min_confidence
(default 0.5), max_rows (default 100), min_similarity (default 0.3). Returns decisions
ordered by similarity (semantic) or relevance_score (fallback). Full args:
okto-pulse://reference/tool-docs/kg."""
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_get_decision_history called: board_id=%s topic=%r", board_id, topic)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_get_decision_history offloading to thread")
            rows = await asyncio.to_thread(
                svc.get_decision_history,
                board_id, topic,
                min_confidence=min_confidence, max_rows=max_rows,
                use_semantic=use_semantic, min_similarity=min_similarity,
            )
            logger.debug("[KG] kg_get_decision_history thread returned: count=%d", len(rows))
            resp = DecisionHistoryResponse(
                decisions=[KGNodeResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_get_related_context(
        board_id: str,
        artifact_id: str,
        min_confidence: float = 0.5,
        max_rows: int = 100,
        rel_types: str = "",
        direction: str = "both",
        max_depth: int = 2,
        graph_layer: str = "canonical",
    ) -> str:
        """
        Given an artifact, return its neighborhood in the KG: prior
        decisions, applicable criteria, similar bugs, discarded alternatives.
        Supports impact-analysis filters so an agent can scope traversal to
        a specific edge set or direction.

        Args:
            board_id: Board ID
            artifact_id: Source artifact reference (source_artifact_ref)
            min_confidence: Minimum confidence (default 0.5)
            max_rows: Maximum results (default 100)
            rel_types: Comma- or pipe-separated edge types to restrict the
                first hop (e.g. ``"supersedes,contradicts"`` or
                ``"tests|relates_to"``). Empty = any type.
            direction: ``"both"`` (default), ``"outgoing"``, or ``"incoming"``.
                Applied to hop1 only; hop2 is always undirected.
            max_depth: ``1`` returns center+hop1 only (hop2 fields null);
                ``2`` (default) returns the full 2-hop context.
            graph_layer: ``canonical`` (default) | ``working`` | ``all`` (spec
                849d6292, FR6). The default scopes the neighborhood to canonical
                nodes so an explored subgraph NEVER leaks ``working`` nodes;
                pass ``working``/``all`` to widen it. Invalid values return a
                structured error.

        Returns:
            JSON with 2-hop neighborhood context + `applied_graph_layer` echoing
            the layer actually applied to the traversal (canonical|working|all).
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_get_related_context called: board_id=%s artifact_id=%s", board_id, artifact_id)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            # R6-IMP3: normalize at the boundary (fail-closed on invalid) + echo the
            # applied layer top-level so the explored scope is auditable.
            applied_layer = normalize_graph_layer(graph_layer)
            parsed_types: list[str] | None = None
            if rel_types:
                tokens = [t.strip() for t in rel_types.replace("|", ",").split(",")]
                parsed_types = [t for t in tokens if t]
            logger.debug("[KG] kg_get_related_context offloading to thread")
            rows = await asyncio.to_thread(
                svc.get_related_context,
                board_id, artifact_id,
                min_confidence=min_confidence, max_rows=max_rows,
                rel_types=parsed_types, direction=direction, max_depth=max_depth,
                graph_layer=applied_layer,
            )
            logger.debug("[KG] kg_get_related_context thread returned: count=%d", len(rows))
            resp = RelatedContextResponse(
                context=[ContextHop(**r) for r in rows],
                count=len(rows),
                applied_graph_layer=applied_layer,
            )
            return resp.model_dump_json()
        except ValueError as e:
            return _err("invalid_argument", str(e))
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_get_supersedence_chain(
        board_id: str,
        decision_id: str,
    ) -> str:
        """
        Trace what superseded what for a specific decision. Returns the
        chain of superseded decisions up to depth 10.

        Args:
            board_id: Board ID
            decision_id: Decision node ID to trace from

        Returns:
            JSON with chain, depth, current_active
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_get_supersedence_chain called: board_id=%s decision_id=%s", board_id, decision_id)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_get_supersedence_chain offloading to thread")
            result = await asyncio.to_thread(svc.get_supersedence_chain, board_id, decision_id)
            logger.debug("[KG] kg_get_supersedence_chain thread returned: depth=%d", result.get("depth", 0))
            resp = SupersedenceChainResponse(
                chain=[SupersedenceEntry(**e) for e in result["chain"]],
                depth=result["depth"],
                current_active=result["current_active"],
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_find_contradictions(
        board_id: str,
        node_id: str = "",
        max_rows: int = 50,
    ) -> str:
        """
        Find contradictory decision pairs via :contradicts relationship.
        When node_id is provided, returns only pairs involving that node.
        Without node_id, returns all contradiction pairs (limit 50).

        Args:
            board_id: Board ID
            node_id: Optional Decision node ID (empty = all pairs)
            max_rows: Maximum pairs (default 50)

        Returns:
            JSON with pairs: [{id_a, title_a, id_b, title_b, confidence}]
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_find_contradictions called: board_id=%s node_id=%s", board_id, node_id)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_find_contradictions offloading to thread")
            rows = await asyncio.to_thread(
                svc.find_contradictions,
                board_id, node_id=node_id or None, max_rows=max_rows,
            )
            logger.debug("[KG] kg_find_contradictions thread returned: count=%d", len(rows))
            resp = ContradictionsResponse(
                pairs=[ContradictionPair(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_find_similar_decisions(
        board_id: str,
        topic: str,
        top_k: int = 10,
        min_similarity: float = 0.3,
    ) -> str:
        """
        Find decisions similar to a topic using hybrid ranking:
        0.5*semantic + 0.2*graph_centrality + 0.2*recency + 0.1*confidence.

        Args:
            board_id: Board ID
            topic: Natural language description to match against
            top_k: Maximum results (default 10)
            min_similarity: Minimum similarity threshold (default 0.3)

        Returns:
            JSON with decisions ordered by combined_score DESC
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_find_similar_decisions called: board_id=%s topic=%r", board_id, topic)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_find_similar_decisions offloading to thread")
            rows = await asyncio.to_thread(
                svc.find_similar_decisions,
                board_id, topic, top_k=top_k, min_similarity=min_similarity,
            )
            logger.debug("[KG] kg_find_similar_decisions thread returned: count=%d", len(rows))
            resp = SimilarDecisionsResponse(
                decisions=[SimilarDecisionResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_explain_constraint(
        board_id: str,
        constraint_id: str,
    ) -> str:
        """
        Explain the origin of a constraint: the spec/decision it derives from,
        related constraints, and any violations (bugs) registered against it.

        Args:
            board_id: Board ID
            constraint_id: Constraint node ID

        Returns:
            JSON with constraint details, origins, and violations
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_explain_constraint called: board_id=%s constraint_id=%s", board_id, constraint_id)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_explain_constraint offloading to thread")
            result = await asyncio.to_thread(svc.explain_constraint, board_id, constraint_id)
            logger.debug("[KG] kg_explain_constraint thread returned: id=%s", result.get("id", "unknown"))
            resp = ConstraintExplanationResponse(
                constraint=ConstraintExplanation(**result),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_list_alternatives(
        board_id: str,
        decision_id: str,
        max_rows: int = 100,
    ) -> str:
        """
        List alternatives that were considered and discarded for a decision,
        including their reason_discarded from the narrative.

        Args:
            board_id: Board ID
            decision_id: Decision node ID
            max_rows: Maximum results (default 100)

        Returns:
            JSON with alternatives list
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_list_alternatives called: board_id=%s decision_id=%s", board_id, decision_id)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_list_alternatives offloading to thread")
            rows = await asyncio.to_thread(
                svc.list_alternatives,
                board_id, decision_id, max_rows=max_rows,
            )
            logger.debug("[KG] kg_list_alternatives thread returned: count=%d", len(rows))
            resp = AlternativesResponse(
                alternatives=[AlternativeResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_get_learning_from_bugs(
        board_id: str,
        area: str,
        min_confidence: float = 0.5,
        max_rows: int = 100,
    ) -> str:
        """
        Get lessons learned from bugs in a specific area. Returns Learning
        nodes connected to Bug nodes via :validates relationship.

        Args:
            board_id: Board ID
            area: Area keyword to filter bugs by (matches title/content)
            min_confidence: Minimum confidence (default 0.5)
            max_rows: Maximum results (default 100)

        Returns:
            JSON with learnings: [{learning_id, learning_title, bug_id, bug_title}]
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_get_learning_from_bugs called: board_id=%s area=%r", board_id, area)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_get_learning_from_bugs offloading to thread")
            rows = await asyncio.to_thread(
                svc.get_learning_from_bugs,
                board_id, area, min_confidence=min_confidence, max_rows=max_rows,
            )
            logger.debug("[KG] kg_get_learning_from_bugs thread returned: count=%d", len(rows))
            resp = LearningsResponse(
                learnings=[LearningResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)

    @mcp.tool()
    async def okto_pulse_kg_query_global(
        board_id: str = "",
        nl_query: str = "",
        top_k: int = 10,
        graph_layer: str = "canonical",
    ) -> str:
        """
        Cross-board semantic search via the global discovery layer. Returns
        matching decisions from all boards the agent has access to, filtered
        by ACL.

        Args:
            board_id: Optional board_id to restrict search (empty = all boards)
            nl_query: Natural language query string
            top_k: Maximum results (default 10)
            graph_layer: canonical, working, or all (default canonical);
                invalid values fail closed.

        Returns:
            JSON `{results: [{board_id, id, title, similarity, graph_layer}],
            count, applied_graph_layer}`. `applied_graph_layer` echoes the layer
            actually applied (canonical|working|all).
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_query_global called: board_id=%s nl_query=%r", board_id, nl_query)
        svc = get_kg_service()
        try:
            # R6-IMP3: normalize at the boundary so an invalid graph_layer fails
            # closed here and the applied (normalized) layer is echoed top-level.
            applied_layer = normalize_graph_layer(graph_layer)
            target_boards = [board_id] if board_id else boards
            logger.debug("[KG] kg_query_global offloading to thread, target_boards=%d", len(target_boards))
            rows = await asyncio.to_thread(
                svc.query_global,
                nl_query,
                user_boards=target_boards,
                top_k=top_k,
                graph_layer=applied_layer,
            )
            logger.debug("[KG] kg_query_global thread returned: count=%d", len(rows))
            resp = GlobalQueryResponse(
                results=[GlobalResult(**r) for r in rows],
                count=len(rows),
                applied_graph_layer=applied_layer,
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err_from(e)
