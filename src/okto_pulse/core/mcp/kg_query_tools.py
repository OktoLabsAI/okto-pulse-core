"""MCP tool wrappers for the 9 tier primario intent-based query tools.

Registered via `register_kg_query_tools(mcp, get_agent, get_db)` called from
server.py. Each tool authenticates, resolves board ACL, delegates to
`kg_service`, and serializes the response as JSON.
"""

from __future__ import annotations

import json
from typing import Any

from okto_pulse.core.kg.kg_service import KGService, KGToolError, get_kg_service
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


def _err(code: str, message: str, **extra: Any) -> str:
    payload: dict = {"error": {"code": code, "message": message}}
    if extra:
        payload["error"].update(extra)
    return json.dumps(payload, default=str)


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
    ) -> str:
        """
        Trace decisions about a topic/module over time. Returns decisions
        matching the topic with their supersedence chain.

        Args:
            board_id: Board ID
            topic: Topic or keyword to search for in decision titles
            min_confidence: Minimum confidence threshold (default 0.5)
            max_rows: Maximum results (default 100)

        Returns:
            JSON with decisions list ordered by created_at DESC
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            rows = svc.get_decision_history(
                board_id, topic, min_confidence=min_confidence, max_rows=max_rows,
            )
            resp = DecisionHistoryResponse(
                decisions=[KGNodeResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

    @mcp.tool()
    async def okto_pulse_kg_get_related_context(
        board_id: str,
        artifact_id: str,
        min_confidence: float = 0.5,
        max_rows: int = 100,
    ) -> str:
        """
        Given a new artifact, return historical context: prior decisions,
        applicable criteria, similar bugs, discarded alternatives.

        Args:
            board_id: Board ID
            artifact_id: Source artifact reference (source_artifact_ref)
            min_confidence: Minimum confidence (default 0.5)
            max_rows: Maximum results (default 100)

        Returns:
            JSON with 2-hop neighborhood context
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            rows = svc.get_related_context(
                board_id, artifact_id, min_confidence=min_confidence, max_rows=max_rows,
            )
            resp = RelatedContextResponse(
                context=[ContextHop(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

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
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            result = svc.get_supersedence_chain(board_id, decision_id)
            resp = SupersedenceChainResponse(
                chain=[SupersedenceEntry(**e) for e in result["chain"]],
                depth=result["depth"],
                current_active=result["current_active"],
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

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
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            rows = svc.find_contradictions(
                board_id, node_id=node_id or None, max_rows=max_rows,
            )
            resp = ContradictionsResponse(
                pairs=[ContradictionPair(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

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
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            rows = svc.find_similar_decisions(
                board_id, topic, top_k=top_k, min_similarity=min_similarity,
            )
            resp = SimilarDecisionsResponse(
                decisions=[SimilarDecisionResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

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
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            result = svc.explain_constraint(board_id, constraint_id)
            resp = ConstraintExplanationResponse(
                constraint=ConstraintExplanation(**result),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

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
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            rows = svc.list_alternatives(
                board_id, decision_id, max_rows=max_rows,
            )
            resp = AlternativesResponse(
                alternatives=[AlternativeResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

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
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            rows = svc.get_learning_from_bugs(
                board_id, area, min_confidence=min_confidence, max_rows=max_rows,
            )
            resp = LearningsResponse(
                learnings=[LearningResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)

    @mcp.tool()
    async def okto_pulse_kg_query_global(
        board_id: str = "",
        nl_query: str = "",
        top_k: int = 10,
    ) -> str:
        """
        Cross-board semantic search via the global discovery layer. Returns
        matching decisions from all boards the agent has access to, filtered
        by ACL.

        Args:
            board_id: Optional board_id to restrict search (empty = all boards)
            nl_query: Natural language query string
            top_k: Maximum results (default 10)

        Returns:
            JSON with results: [{board_id, id, title, similarity}]
        """
        agent, boards = await _get_user_boards(get_agent, get_db)
        if agent is None:
            return _err("unauthorized", "authentication required")
        svc = get_kg_service()
        try:
            target_boards = [board_id] if board_id else boards
            rows = svc.query_global(nl_query, user_boards=target_boards, top_k=top_k)
            resp = GlobalQueryResponse(
                results=[GlobalResult(**r) for r in rows],
                count=len(rows),
            )
            return resp.model_dump_json()
        except KGToolError as e:
            return _err(e.code, e.message)
