"""MCP tool wrappers for the 7 consolidation primitives.

Tools are registered via `register_kg_tools(mcp)` which is called from
`server.py` AFTER the `mcp` FastMCP instance is constructed. This avoids the
circular import that would happen if `kg_tools` imported `mcp` at module load.

Each tool:
1. Resolves the authenticated agent (shared helper from server.py)
2. Validates the request payload against the Pydantic schema
3. Delegates to the corresponding primitive function
4. Serializes the response (or error) as JSON string
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    abort_consolidation,
    add_edge_candidate,
    add_node_candidate,
    begin_consolidation,
    commit_consolidation,
    get_similar_nodes,
    propose_reconciliation,
)
from okto_pulse.core.kg.schemas import (
    AbortConsolidationRequest,
    AddEdgeCandidateRequest,
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    GetSimilarNodesRequest,
    ProposeReconciliationRequest,
)


def _err(code: str, message: str, **extra: Any) -> str:
    payload: dict[str, Any] = {"error": {"code": code, "message": message}}
    if extra:
        payload["error"].update(extra)
    return json.dumps(payload, default=str)


def _ok(response) -> str:
    return response.model_dump_json()


def register_kg_tools(mcp, *, get_agent, get_db) -> None:
    """Register the 7 KG primitive tools on the given FastMCP instance.

    Args:
        mcp: FastMCP instance from okto_pulse.core.mcp.server
        get_agent: async callable returning the authenticated agent, or None
                   on auth failure (shared helper from server.py)
        get_db: async context manager yielding an AsyncSession
    """

    @mcp.tool()
    async def okto_pulse_kg_begin_consolidation(
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        raw_content: str,
        deterministic_candidates: list[dict] | None = None,
    ) -> str:
        """
        Open a transactional consolidation session against a board.

        Computes SHA256(board + artifact + content) for nothing-changed detection.
        Returns a session_id the agent uses in all subsequent primitives. The
        session has a TTL (default 1h, configurable via kg_session_ttl_seconds)
        and is owned exclusively by the authenticated agent.

        Args:
            board_id: Target board
            artifact_type: spec | sprint | qa | etc.
            artifact_id: Source artifact id
            raw_content: Full artifact content used for SHA256 dedup
            deterministic_candidates: Pre-extracted node candidates (ORNs, refs)

        Returns:
            JSON with session_id, content_hash, nothing_changed flag, expires_at
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = BeginConsolidationRequest(
                board_id=board_id,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                raw_content=raw_content,
                deterministic_candidates=deterministic_candidates or [],
            )
        except ValidationError as e:
            return _err("invalid_candidate", str(e))
        async with get_db() as db:
            try:
                resp = await begin_consolidation(req, agent_id=agent.id, db=db)
                return _ok(resp)
            except KGPrimitiveError as e:
                return _err(e.code, e.message, session_id=e.session_id,
                            details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_add_node_candidate(
        session_id: str,
        candidate: dict,
    ) -> str:
        """
        Add a node candidate to an open consolidation session.

        The candidate stays in-memory until commit_consolidation or expiry.
        candidate_id must be unique within the session.

        Args:
            session_id: Session from begin_consolidation
            candidate: Dict with candidate_id, node_type, title, content, etc.

        Returns:
            JSON with accepted=true and node_count_in_session
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = AddNodeCandidateRequest(session_id=session_id, candidate=candidate)
        except ValidationError as e:
            return _err("invalid_candidate", str(e))
        try:
            resp = await add_node_candidate(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_add_edge_candidate(
        session_id: str,
        candidate: dict,
    ) -> str:
        """
        Add an edge candidate to an open session.

        Endpoints (from_candidate_id / to_candidate_id) must reference either
        another in-session node candidate OR an existing Kùzu node via the
        'kg:' prefix (kg:decision_abc123).

        Args:
            session_id: Session from begin_consolidation
            candidate: Dict with candidate_id, edge_type, from/to, confidence

        Returns:
            JSON with accepted=true and edge_count_in_session
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = AddEdgeCandidateRequest(session_id=session_id, candidate=candidate)
        except ValidationError as e:
            return _err("invalid_candidate", str(e))
        try:
            resp = await add_edge_candidate(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_get_similar_nodes(
        session_id: str,
        candidate_id: str,
        top_k: int = 5,
        min_similarity: float = 0.3,
    ) -> str:
        """
        Fetch existing Kùzu nodes similar to an in-session candidate.

        MVP uses title-prefix match as a deterministic fallback; production
        replaces with HNSW k-NN via vector index (card 00dae72a).

        Args:
            session_id: Session from begin_consolidation
            candidate_id: Candidate to compare against
            top_k: Max neighbors (1-50, default 5)
            min_similarity: Threshold (0.0-1.0, default 0.3)

        Returns:
            JSON with similar: [SimilarNode]
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = GetSimilarNodesRequest(
                session_id=session_id,
                candidate_id=candidate_id,
                top_k=top_k,
                min_similarity=min_similarity,
            )
        except ValidationError as e:
            return _err("invalid_candidate", str(e))
        try:
            resp = await get_similar_nodes(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_propose_reconciliation(
        session_id: str,
    ) -> str:
        """
        Compute deterministic ADD/UPDATE/SUPERSEDE/NOOP hints for every candidate.

        Rules:
        - SHA256 matches last commit → NOOP for all candidates
        - Otherwise → ADD with candidate's self-assessed confidence

        UPDATE/SUPERSEDE hints will land once the HNSW index is in place.

        Args:
            session_id: Session from begin_consolidation

        Returns:
            JSON with hints: [ReconciliationHint]
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = ProposeReconciliationRequest(session_id=session_id)
        except ValidationError as e:
            return _err("invalid_candidate", str(e))
        async with get_db() as db:
            try:
                resp = await propose_reconciliation(req, agent_id=agent.id, db=db)
                return _ok(resp)
            except KGPrimitiveError as e:
                return _err(e.code, e.message, session_id=e.session_id,
                            details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_commit_consolidation(
        session_id: str,
        summary_text: str = "",
        agent_overrides: dict[str, dict] | None = None,
    ) -> str:
        """
        Atomically commit the session: Kùzu writes + audit row + outbox event.

        agent_overrides map candidate_id → ReconciliationHint for cases where
        the agent's semantic reasoning produces a different op than the
        server's deterministic default.

        Args:
            session_id: Session from begin_consolidation
            summary_text: Optional session summary (surfaced in dashboard)
            agent_overrides: Optional per-candidate hint overrides

        Returns:
            JSON with session_id, status=committed, counts, committed_at
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = CommitConsolidationRequest(
                session_id=session_id,
                summary_text=summary_text or None,
                agent_overrides=agent_overrides or {},
            )
        except ValidationError as e:
            return _err("invalid_candidate", str(e))
        async with get_db() as db:
            try:
                resp = await commit_consolidation(req, agent_id=agent.id, db=db)
                return _ok(resp)
            except KGPrimitiveError as e:
                return _err(e.code, e.message, session_id=e.session_id,
                            details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_abort_consolidation(
        session_id: str,
        reason: str = "",
    ) -> str:
        """
        Drop an in-flight session without committing.

        No compensating delete is applied — commit was never called, so Kùzu
        has no partial writes. The session is marked aborted and removed from
        the in-memory registry.

        Args:
            session_id: Session from begin_consolidation
            reason: Optional reason (logged for audit)

        Returns:
            JSON with session_id, status=aborted
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = AbortConsolidationRequest(
                session_id=session_id, reason=reason or None
            )
        except ValidationError as e:
            return _err("invalid_candidate", str(e))
        try:
            resp = await abort_consolidation(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)
