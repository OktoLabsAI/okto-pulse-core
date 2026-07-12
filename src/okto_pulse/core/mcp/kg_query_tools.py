"""MCP tool wrappers for the 9 tier primario intent-based query tools.

Registered via `register_kg_query_tools(mcp, get_agent, get_uow)` called from
server.py. Each tool authenticates and resolves board ACL through the registered
AuthContext factory. If the composition root did not provide AuthContext, the
tool family fails closed instead of consulting a local relational ACL fallback.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.application.use_cases import ActorContext
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


_RELATED_CONTEXT_TYPED_REF_PREFIXES = frozenset({"spec", "card"})


def _raw_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except (TypeError, ValueError):
        return False


def _validate_related_context_artifact_ref(artifact_id: str) -> str | None:
    """Validate KG related-context anchors at the MCP boundary.

    ``spec:<uuid>`` and ``card:<uuid>`` are unambiguous and supported. Existing
    non-UUID artifact refs remain accepted for backward compatibility with
    historical ``source_artifact_ref`` values. Raw UUIDs fail before any graph
    store query so the core does not infer relational type.
    """
    value = (artifact_id or "").strip()
    if not value:
        return "artifact_id is required. Use spec:<uuid> or card:<uuid>."
    if ":" in value:
        prefix, raw_id = value.split(":", 1)
        if prefix not in _RELATED_CONTEXT_TYPED_REF_PREFIXES or not raw_id:
            return (
                "artifact_id must use a typed reference: spec:<uuid> or card:<uuid>. "
                "Do not pass raw UUIDs."
            )
        return None
    if _raw_uuid(value):
        return (
            "artifact_id is ambiguous as a raw UUID. Use spec:<uuid> or card:<uuid> "
            "so KG queries do not infer relational type."
        )
    return None


async def _get_auth_context():
    """Get AuthContext from registry or return None."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    factory = get_kg_registry().auth_context_factory
    if factory is None:
        return None
    return factory()


async def _get_user_boards(get_agent=None, get_uow=None) -> tuple[Any, list[str]]:
    """Authenticate agent and return (agent_id, board_ids) via AuthContext.

    ``get_agent`` and ``get_uow`` remain in the call signature for the MCP
    registration contract, but are intentionally not used as a fallback. R06
    requires a registered AuthContext/port or a fail-closed unauthenticated
    envelope.
    """
    _ = (get_agent, get_uow)
    auth = await _get_auth_context()
    if auth is not None:
        agent_id = await auth.get_agent_id()
        if agent_id is None:
            return None, []

        class _Stub:
            def __init__(self, id):
                self.id = id

        boards = list(await auth.get_accessible_boards() or [])
        realm_scope = await auth.get_realm_scope()
        query_scope = ActorScope.from_context(
            ActorContext(agent_id, "mcp", realm_scope=realm_scope)
        ).query_scope(allowed_board_ids=boards, require_ownership=False)
        return _Stub(agent_id), sorted(query_scope.allowed_board_ids or ())

    return None, []


def register_kg_query_tools(mcp, *, get_agent, get_uow) -> None:
    """Register the 9 tier primario query tools on the command catalog.

    Spec R01A MCP-FU4: the shared ``_get_user_boards`` helper is injected with the
    MCP UnitOfWorkFactory (``get_uow``) instead of a raw ``get_db`` session source,
    so no tool in this family opens a relational session directly.
    """

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
        agent, boards = await _get_user_boards(get_agent, get_uow)
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
        Return a bounded KG neighborhood for an artifact: decisions, criteria,
        bugs and alternatives. Supports edge filters, direction, max_depth and
        graph_layer (`canonical` default, `working`, or `all`); invalid layer
        fails closed. Response echoes `applied_graph_layer`.
        Full docs: okto-pulse://reference/tool-docs/kg.
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_get_related_context called: board_id=%s artifact_id=%s", board_id, artifact_id)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            # R6-IMP3: normalize at the boundary (fail-closed on invalid) + echo the
            # applied layer top-level so the explored scope is auditable.
            applied_layer = normalize_graph_layer(graph_layer)
            ref_error = _validate_related_context_artifact_ref(artifact_id)
            if ref_error:
                return _err(
                    "invalid_artifact_ref",
                    ref_error,
                    supported=["spec:<uuid>", "card:<uuid>"],
                    examples=["spec:11111111-1111-1111-1111-111111111111"],
                )
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
        node_type: str = "Decision",
    ) -> str:
        """Trace what superseded what for a node (default node_type Decision), up to
        depth 10. Returns chain, depth and current_active.
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_get_supersedence_chain called: board_id=%s decision_id=%s", board_id, decision_id)
        svc = get_kg_service()
        try:
            svc.check_board_access(boards, board_id)
            logger.debug("[KG] kg_get_supersedence_chain offloading to thread")
            result = await asyncio.to_thread(
                svc.get_supersedence_chain, board_id, decision_id, node_type
            )
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
        """Find contradictory decision pairs via the :contradicts relationship. With
        node_id, returns only pairs involving that node; without it, all pairs
        (max_rows default 50).
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
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
        """Find decisions similar to a natural-language topic using hybrid ranking
        (0.5*semantic + 0.2*graph_centrality + 0.2*recency + 0.1*confidence),
        ordered by combined_score DESC.
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
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
        """Explain the origin of a constraint: the spec/decision it derives from,
        related constraints, and any violations (bugs) registered against it.
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
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
        """List alternatives that were considered and discarded for a decision,
        including their reason_discarded from the narrative (max_rows default
        100).
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
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
        """Get lessons learned from bugs in an area (keyword match on
        title/content). Returns Learning nodes connected to Bug nodes via the
        :validates relationship, filtered by min_confidence (default 0.5).
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
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
        """Cross-board semantic search via the global discovery layer. Returns
        matching decisions from all boards the agent has access to, filtered by
        ACL; optional board_id restricts to one board (empty = all). graph_layer
        accepts canonical|working|all (default canonical); invalid values fail
        closed. The response echoes applied_graph_layer.
        """
        agent, boards = await _get_user_boards(get_agent, get_uow)
        if agent is None:
            return _err("unauthorized", "authentication required")
        logger.debug("[KG] kg_query_global called: board_id=%s nl_query=%r", board_id, nl_query)
        svc = get_kg_service()
        try:
            # R6-IMP3: normalize at the boundary so an invalid graph_layer fails
            # closed here and the applied (normalized) layer is echoed top-level.
            applied_layer = normalize_graph_layer(graph_layer)
            if board_id:
                svc.check_board_access(boards, board_id)
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
