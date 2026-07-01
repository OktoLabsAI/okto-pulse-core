"""The 7 consolidation primitives — pure async functions (no MCP decoration).

Each primitive takes a typed Pydantic request and returns a typed response.
The MCP layer in `okto_pulse.core.mcp.kg_tools` wraps these and handles
auth/serialization. Keeping the primitives decoupled from MCP means the REST
API (spec 3681b078) can reuse the same functions.

Reconciliation rules (deterministic, zero LLM):
- SHA256 content_hash matches last committed → NOOP
- Candidate has stable id that matches existing kuzu node → UPDATE
- Candidate similar to existing node (embedding + title fuzzy match) but
  new id → SUPERSEDE hint, agent decides whether to override
- Otherwise → ADD

commit_consolidation writes to Kùzu first (via compensating delete on
failure — the pattern lives in card 7b922175 `compensating_tx.py`), then
writes the audit row + outbox event in a single SQLite transaction.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import partial

from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
    CONNECTIVITY_ERROR_CODE,
    DEGRADED_KG_STATES,
    KGConnectivityEdgeGroup,
    KGConnectivityEdgeRequirement,
    KGConnectivityMetricEvent,
    KGConnectivityRuleRegistry,
    KGNodeConnectivityGuard,
    KGNodeRef,
    MetricSinkProtocol,
)
from okto_pulse.core.kg.schemas import (
    AbortConsolidationRequest,
    AbortConsolidationResponse,
    AddEdgeCandidateRequest,
    AddEdgeCandidateResponse,
    AddNodeCandidateRequest,
    AddNodeCandidateResponse,
    BeginConsolidationRequest,
    BeginConsolidationResponse,
    CommitConsolidationRequest,
    CommitConsolidationResponse,
    EdgeCandidate,
    GetSimilarNodesRequest,
    GetSimilarNodesResponse,
    KGEdgeType,
    ProposeReconciliationRequest,
    ProposeReconciliationResponse,
    ReconciliationHint,
    ReconciliationOperation,
    SessionStatus,
    SimilarNode,
)
from okto_pulse.core.kg.session_manager import (
    ConsolidationSession,
    compute_content_hash,
)

logger = logging.getLogger("okto_pulse.kg.primitives")


def _allowed_edge_pairs(edge_type: str) -> tuple[tuple[str, str], ...]:
    from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, REL_TYPES

    pairs = [(from_type, to_type) for rel, from_type, to_type in REL_TYPES if rel == edge_type]
    for rel, multi_pairs in MULTI_REL_TYPES:
        if rel == edge_type:
            pairs.extend(multi_pairs)
    return tuple(pairs)


def _validate_local_edge_pair(
    edge_type: str,
    from_type: str | None,
    to_type: str | None,
    *,
    session_id: str,
) -> None:
    if not from_type or not to_type:
        return
    allowed = _allowed_edge_pairs(edge_type)
    if not allowed or (from_type, to_type) in allowed:
        return
    expected = ", ".join(f"{src}->{dst}" for src, dst in allowed)
    raise KGPrimitiveError(
        "invalid_edge_endpoint_types",
        (
            f"edge_type '{edge_type}' cannot connect {from_type}->{to_type}; "
            f"allowed endpoint pair(s): {expected}. "
            "Use only schema-supported cognitive edges; deterministic edges "
            "such as implements/tests/belongs_to are owned by the worker."
        ),
        session_id=session_id,
        details={
            "edge_type": edge_type,
            "from_type": from_type,
            "to_type": to_type,
            "allowed_pairs": [
                {"from_type": src, "to_type": dst}
                for src, dst in allowed
            ],
        },
    )

# ---------------------------------------------------------------------------
# Thread pool for offloading synchronous Kùzu operations from the event loop
# Lazy-initialized to avoid thread leaks in test suites.
# ---------------------------------------------------------------------------

_kuzu_executor: ThreadPoolExecutor | None = None


def _get_kuzu_executor() -> ThreadPoolExecutor:
    global _kuzu_executor
    if _kuzu_executor is None:
        _kuzu_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="kuzu",
        )
    return _kuzu_executor


async def _run_kuzu(func, *args, **kwargs):
    """Run a synchronous Kùzu operation in a dedicated thread pool."""
    loop = asyncio.get_running_loop()
    executor = _get_kuzu_executor()
    if kwargs:
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return await loop.run_in_executor(executor, func, *args)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class KGPrimitiveError(Exception):
    def __init__(self, code: str, message: str, session_id: str | None = None,
                 details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.session_id = session_id
        self.details = details or {}


KG_GRAPH_DEGRADED_ERROR_CODE = "kg_graph_degraded"


class _LoggingConnectivityMetricSink:
    """Emit connectivity guard telemetry using safe metric labels only."""

    def emit(self, event: KGConnectivityMetricEvent) -> None:
        labels = event.labels()
        logger.info(
            "kg.connectivity.guard board=%s node_type=%s writer_path=%s "
            "outcome=%s reason=%s source_resolution_status=%s generation=%s",
            labels["board_id"],
            labels["node_type"],
            labels["writer_path"],
            labels["outcome"],
            labels["reason"],
            labels["source_resolution_status"],
            labels["generation_id"],
            extra={
                "event": "kg_node_connectivity_guard_total",
                **labels,
            },
        )
        if labels["outcome"] == "rejected":
            logger.info(
                "kg.connectivity.orphan_rejected board=%s node_type=%s "
                "writer_path=%s reason=%s source_resolution_status=%s",
                labels["board_id"],
                labels["node_type"],
                labels["writer_path"],
                labels["reason"],
                labels["source_resolution_status"],
                extra={
                    "event": "kg_orphan_node_rejected_total",
                    **labels,
                },
            )
        if labels["source_resolution_status"] == "deferred_degraded_graph":
            logger.info(
                "kg.connectivity.deferred board=%s node_type=%s "
                "writer_path=%s reason=%s",
                labels["board_id"],
                labels["node_type"],
                labels["writer_path"],
                labels["reason"],
                extra={
                    "event": "kg_connectivity_deferred_total",
                    **labels,
                },
            )


_CONNECTIVITY_METRIC_SINK: MetricSinkProtocol = _LoggingConnectivityMetricSink()


def _connectivity_metric_sink() -> MetricSinkProtocol:
    return _CONNECTIVITY_METRIC_SINK


def _contextualize_ladybug_commit_error(exc: BaseException) -> tuple[str, dict]:
    """Turn low-level Ladybug buffer errors into actionable operator context."""
    msg = str(exc)
    lower = msg.lower()
    if not any(
        marker in lower
        for marker in (
            "buffer manager",
            "buffer pool",
            "unable to allocate memory",
            "power of 2",
            "power-of-2",
        )
    ):
        return msg, {}

    details: dict = {}
    try:
        from okto_pulse.core.infra.config import get_settings

        s = get_settings()
        details = {
            "kg_kuzu_buffer_pool_mb": s.kg_kuzu_buffer_pool_mb,
            "kg_kuzu_max_db_size_gb": s.kg_kuzu_max_db_size_gb,
        }
        if "power of 2" in lower or "power-of-2" in lower:
            hint = (
                "Graph DB max database size per board is invalid for Ladybug. "
                "Use one of 2, 4, 8, 16, 32 or 64 GB; even values such as "
                f"{s.kg_kuzu_max_db_size_gb} GB may still be invalid unless "
                "they are powers of 2."
            )
        else:
            hint = (
                "Graph DB buffer pool was exhausted during consolidation. "
                "This can leave a partial WAL behind; set "
                "kg_kuzu_buffer_pool_mb=512 in Settings, restart Okto Pulse, "
                "and retry the consolidation before reprocessing dead letters. "
                f"Current settings: buffer={s.kg_kuzu_buffer_pool_mb}MB, "
                f"max_db={s.kg_kuzu_max_db_size_gb}GB."
            )
    except Exception:
        hint = (
            "Graph DB buffer/max-size settings may be invalid. Review Settings "
            "and restart before retrying consolidation."
        )
    return f"{msg}. {hint}", details


def _not_found(session_id: str) -> KGPrimitiveError:
    return KGPrimitiveError(
        "session_not_found",
        f"Session not found or expired: {session_id}",
        session_id=session_id,
    )


def _ownership(session_id: str, agent_id: str) -> KGPrimitiveError:
    return KGPrimitiveError(
        "session_ownership_mismatch",
        f"Agent {agent_id} does not own session {session_id}",
        session_id=session_id,
    )


async def _require_open_session(
    session_id: str, agent_id: str
) -> ConsolidationSession:
    store = get_kg_registry().require_session_store()
    session = await store.get(session_id)
    if session is None:
        raise _not_found(session_id)
    if not session.check_ownership(agent_id):
        raise _ownership(session_id, agent_id)
    if session.status != SessionStatus.OPEN:
        raise KGPrimitiveError(
            "session_already_committed",
            f"Session {session_id} is in status {session.status}",
            session_id=session_id,
        )
    return session


# ---------------------------------------------------------------------------
# 1. begin_consolidation
# ---------------------------------------------------------------------------


async def begin_consolidation(
    req: BeginConsolidationRequest,
    *,
    agent_id: str,
    db=None,
    force_reprocess: bool = False,
) -> BeginConsolidationResponse:
    """Open a new transactional session. SHA256-dedup against the last commit."""
    registry = get_kg_registry()
    store = registry.require_session_store()
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"

    content_hash = compute_content_hash(req.raw_content, req.artifact_id, req.board_id)

    # Nothing-changed detection is owned by the composed AuditRepository.
    # R-P2-02 removed the silent DB fallback: a runtime without audit_repo is
    # mis-composed and must fail early instead of reading relational tables via
    # an escape path.
    has_audit_source = not force_reprocess
    if has_audit_source:
        latest = await _get_latest_audit(registry, db, req.board_id, req.artifact_id)
        nothing_changed = bool(latest and _audit_hash(latest) == content_hash)
        previous_session_id = _audit_session_id(latest) if latest else None
    else:
        nothing_changed = False
        previous_session_id = None

    # Spec 4007e4a3 (Ideação #3, FR6): structured counter for the
    # nothing_changed short-circuit. Lets observability tooling track how
    # often the begin_consolidation idempotency path saves downstream
    # extraction + reconciliation work for unchanged artifacts.
    if nothing_changed:
        logger.info(
            "kg.consolidation.nothing_changed.short_circuit board=%s "
            "artifact_type=%s artifact_id=%s previous_session=%s",
            req.board_id, req.artifact_type, req.artifact_id, previous_session_id,
            extra={
                "event": "kg.consolidation.nothing_changed.short_circuit",
                "board_id": req.board_id,
                "artifact_type": req.artifact_type,
                "artifact_id": req.artifact_id,
                "previous_session_id": previous_session_id,
            },
        )

    session = await store.create(
        session_id=session_id,
        board_id=req.board_id,
        artifact_id=req.artifact_id,
        artifact_type=req.artifact_type,
        agent_id=agent_id,
        raw_content=req.raw_content,
    )

    for cand in req.deterministic_candidates:
        if cand.candidate_id in session.node_candidates:
            raise KGPrimitiveError(
                "duplicate_candidate_id",
                f"Duplicate deterministic candidate: {cand.candidate_id}",
                session_id=session_id,
            )
        session.node_candidates[cand.candidate_id] = cand

    return BeginConsolidationResponse(
        session_id=session_id,
        board_id=req.board_id,
        artifact_id=req.artifact_id,
        artifact_type=req.artifact_type,
        status=SessionStatus.OPEN,
        content_hash=content_hash,
        nothing_changed=nothing_changed,
        previous_session_id=previous_session_id,
        expires_at=session.expires_at,
        deterministic_candidates_count=len(req.deterministic_candidates),
    )


# ---------------------------------------------------------------------------
# 2. add_node_candidate
# ---------------------------------------------------------------------------


async def add_node_candidate(
    req: AddNodeCandidateRequest,
    *,
    agent_id: str,
) -> AddNodeCandidateResponse:
    from okto_pulse.core.kg.cognitive_policy import (
        COGNITIVE_NODE_CANONICAL_CODE,
        CognitiveNodeLayerError,
        check_cognitive_node_canonical,
    )
    from okto_pulse.core.kg.source_maturity import (
        GRAPH_LAYER_CANONICAL,
        MATURITY_CANONICAL_ELIGIBLE,
    )

    session = await _require_open_session(req.session_id, agent_id)
    store = get_kg_registry().require_session_store()
    cand = req.candidate

    # Cognitive canonical invariant (spec 007d1308 — FR1/FR3/FR4,
    # dec_0b3368fe/dec_26c5cc2d). The cognitive agent only ever produces
    # canonical knowledge; a working-layer node may originate solely from the
    # Layer 1 deterministic worker (agent_id prefixed "system:"), which
    # materializes immature sources per source_maturity (FR5). Enforce in the
    # core primitive — not just the MCP wrapper — so an internal caller cannot
    # bypass it, and BEFORE mutating the session so a rejected candidate leaves
    # no trace in session.node_candidates (TR1/TR4).
    is_system_worker = agent_id.startswith("system:")
    graph_layer_value = (
        cand.graph_layer.value if hasattr(cand.graph_layer, "value") else cand.graph_layer
    )
    try:
        check_cognitive_node_canonical(
            graph_layer_value, is_system_worker=is_system_worker,
        )
    except CognitiveNodeLayerError as exc:
        raise KGPrimitiveError(
            COGNITIVE_NODE_CANONICAL_CODE,
            str(exc),
            session_id=req.session_id,
            details={
                "graph_layer": graph_layer_value,
                "required_graph_layer": exc.required_graph_layer,
                "candidate_id": cand.candidate_id,
            },
        ) from exc

    async with session.lock:
        if cand.candidate_id in session.node_candidates:
            raise KGPrimitiveError(
                "duplicate_candidate_id",
                f"candidate_id already in session: {cand.candidate_id}",
                session_id=req.session_id,
            )
        # FR3 / dec_26c5cc2d: a persisted cognitive candidate is always
        # canonical + canonical_eligible, even when the request omitted or
        # under-specified the fields. The deterministic worker's working nodes
        # (system:*) are left untouched (FR5).
        if not is_system_worker:
            cand.graph_layer = GRAPH_LAYER_CANONICAL
            cand.maturity_status = MATURITY_CANONICAL_ELIGIBLE
        session.node_candidates[cand.candidate_id] = cand
        session.touch(store.default_ttl_seconds)
        return AddNodeCandidateResponse(
            session_id=req.session_id,
            candidate_id=cand.candidate_id,
            accepted=True,
            node_count_in_session=len(session.node_candidates),
        )


# ---------------------------------------------------------------------------
# 3. add_edge_candidate
# ---------------------------------------------------------------------------


async def add_edge_candidate(
    req: AddEdgeCandidateRequest,
    *,
    agent_id: str,
) -> AddEdgeCandidateResponse:
    session = await _require_open_session(req.session_id, agent_id)
    store = get_kg_registry().require_session_store()
    async with session.lock:
        cand = req.candidate

        # Layer ownership: reject deterministic edge types proposed by the
        # cognitive agent (BR `Layer Ownership Isolation` — spec c48a5c33).
        # Local workers set agent_id="system:layer1_worker"; the check only
        # fires for real cognitive sessions.
        from okto_pulse.core.kg.cognitive_policy import (
            DETERMINISTIC_EDGE_TYPES,
            LayerViolationError,
        )
        edge_type_str = (
            cand.edge_type.value if hasattr(cand.edge_type, "value") else cand.edge_type
        )
        is_system_worker = agent_id.startswith("system:")
        if (not is_system_worker) and edge_type_str in DETERMINISTIC_EDGE_TYPES:
            # S-KG-01 / BR-KG-02: a cognitive writer attempting a deterministic
            # edge (e.g. belongs_to) is fail-closed with the bounded reason so
            # callers can branch on it without parsing the message.
            violation = LayerViolationError(edge_type_str)
            raise KGPrimitiveError(
                "layer_violation",
                str(violation),
                session_id=req.session_id,
                details={
                    "reason": violation.reason,
                    "edge_type": violation.edge_type,
                    "allowed_edges": violation.allowed_edges,
                },
            )

        for ep in (cand.from_candidate_id, cand.to_candidate_id):
            if ep.startswith("kg:"):
                continue
            if ep in session.node_candidates:
                continue
            # Cross-session deterministic refs (Layer 1 hierarchy backbone):
            # `<type>_<short>_entity` points to an Entity committed in a prior
            # session of this board. The actual id resolution is deferred to
            # commit_consolidation via Kùzu lookup; here we just accept the
            # shape so the queue worker can stage edges before parents land.
            if _is_cross_session_entity_ref(ep):
                continue
            raise KGPrimitiveError(
                "invalid_candidate",
                f"edge references unknown candidate: {ep}",
                session_id=req.session_id,
            )

        from_local = session.node_candidates.get(cand.from_candidate_id)
        to_local = session.node_candidates.get(cand.to_candidate_id)
        _validate_local_edge_pair(
            edge_type_str,
            _enum_value(from_local.node_type) if from_local else None,
            _enum_value(to_local.node_type) if to_local else None,
            session_id=req.session_id,
        )

        if cand.candidate_id in session.edge_candidates:
            raise KGPrimitiveError(
                "duplicate_candidate_id",
                f"edge candidate_id already in session: {cand.candidate_id}",
                session_id=req.session_id,
            )
        session.edge_candidates[cand.candidate_id] = cand
        session.touch(store.default_ttl_seconds)
        return AddEdgeCandidateResponse(
            session_id=req.session_id,
            candidate_id=cand.candidate_id,
            accepted=True,
            edge_count_in_session=len(session.edge_candidates),
        )


# ---------------------------------------------------------------------------
# 4. get_similar_nodes
# ---------------------------------------------------------------------------


async def get_similar_nodes(
    req: GetSimilarNodesRequest,
    *,
    agent_id: str,
) -> GetSimilarNodesResponse:
    """Return up to top_k existing Kùzu nodes similar to the candidate.

    Embeds the candidate with the active embedding provider (core stub or
    edition-owned concrete adapter) and runs a k-NN query against the per-type HNSW
    index via `kg.search.find_similar_nodes_by_type`. Returns an empty list
    if the index doesn't exist yet or the node type isn't searchable — the
    agent can still proceed with ADD in that case.
    """
    from okto_pulse.core.kg.search import find_similar_nodes_by_type

    session = await _require_open_session(req.session_id, agent_id)
    if req.candidate_id not in session.node_candidates:
        raise KGPrimitiveError(
            "candidate_not_found",
            f"unknown candidate: {req.candidate_id}",
            session_id=req.session_id,
        )

    cand = session.node_candidates[req.candidate_id]
    embedder = get_kg_registry().require_embedding_provider()
    query_vec = embedder.encode(f"{cand.title}\n{cand.content or ''}")

    node_type = (
        cand.node_type.value if hasattr(cand.node_type, "value") else cand.node_type
    )
    raw = await _run_kuzu(
        find_similar_nodes_by_type,
        board_id=session.board_id,
        node_type=node_type,
        query_vector=query_vec,
        top_k=req.top_k,
        min_similarity=req.min_similarity,
    )

    similar = [
        SimilarNode(
            kuzu_node_id=r.kuzu_node_id,
            node_type=r.node_type,
            title=r.title,
            source_artifact_ref=r.source_artifact_ref,
            similarity=r.similarity,
        )
        for r in raw
    ]
    return GetSimilarNodesResponse(
        session_id=req.session_id,
        candidate_id=req.candidate_id,
        similar=similar,
    )


# ---------------------------------------------------------------------------
# 5. propose_reconciliation
# ---------------------------------------------------------------------------


def _find_existing_kuzu_matches(
    board_id: str, node_candidates: dict, embedder,
) -> dict[str, list]:
    """Sync: find existing graph nodes matching session candidates.

    Runs in the thread pool via ``_run_kuzu``.
    """
    from okto_pulse.core.kg.search import find_similar_for_candidate

    existing_matches: dict[str, list] = {}
    try:
        for cand_id, cand in node_candidates.items():
            node_type = (
                cand.node_type.value
                if hasattr(cand.node_type, "value")
                else cand.node_type
            )
            query_vec = embedder.encode(f"{cand.title}\n{cand.content or ''}")
            matches = find_similar_for_candidate(
                board_id=board_id,
                node_type=node_type,
                query_vector=query_vec,
                top_k=5,
                min_similarity=0.3,
            )
            if matches:
                existing_matches[cand_id] = matches
    except Exception as exc:
        logger.warning(
            "kg.primitives.reconciliation_search_failed err=%s", exc,
        )
    return existing_matches


async def propose_reconciliation(
    req: ProposeReconciliationRequest,
    *,
    agent_id: str,
    db=None,
    force_reprocess: bool = False,
) -> ProposeReconciliationResponse:
    """Compute deterministic ADD/UPDATE/SUPERSEDE/NOOP hints for all candidates."""
    from okto_pulse.core.kg.reconciliation import reconcile_session

    registry = get_kg_registry()
    session = await _require_open_session(req.session_id, agent_id)

    if not force_reprocess:
        latest = await _get_latest_audit(
            registry, db, session.board_id, session.artifact_id
        )
        nothing_changed = bool(latest and _audit_hash(latest) == session.content_hash)
    else:
        nothing_changed = False

    existing_matches_by_candidate: dict[str, list] = {}
    if not nothing_changed:
        embedder = registry.require_embedding_provider()
        existing_matches_by_candidate = await _run_kuzu(
            _find_existing_kuzu_matches,
            session.board_id,
            dict(session.node_candidates),
            embedder,
        )

    hints_by_cid = reconcile_session(
        session.node_candidates,
        nothing_changed=nothing_changed,
        existing_matches_by_candidate=existing_matches_by_candidate,
    )
    hints = list(hints_by_cid.values())

    async with session.lock:
        session.reconciliation_hints = hints_by_cid
        session.touch(registry.require_session_store().default_ttl_seconds)

    return ProposeReconciliationResponse(session_id=req.session_id, hints=hints)


# ---------------------------------------------------------------------------
# 6. commit_consolidation
# ---------------------------------------------------------------------------


def _compensate_kuzu_writes(board_id: str, session_id: str, records: list) -> None:
    """Sync: reverse graph writes for a failed commit.

    Mirrors ``TransactionOrchestrator.compensate()`` but runs synchronously
    inside the thread pool. Best-effort — logs failures but does not raise.
    """
    from okto_pulse.core.kg.schema_contract import (
        MULTI_REL_TYPES,
        REL_TYPES,
    )

    async def _run() -> None:
        async with await get_kg_registry().graph_transaction.begin(board_id) as scope:
            # Delete edges first (they reference nodes)
            rel_pairs = list(REL_TYPES)
            for rel_name, endpoint_pairs in MULTI_REL_TYPES:
                rel_pairs.extend(
                    (rel_name, from_type, to_type)
                    for from_type, to_type in endpoint_pairs
                )
            for rel_name, from_type, to_type in rel_pairs:
                try:
                    scope.execute(
                        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
                        f"WHERE r.created_by_session_id = $sid DELETE r",
                        {"sid": session_id},
                    )
                except Exception:
                    pass

            # Delete nodes
            node_types = {r.entity_type for r in records if r.kind == "node"}
            for node_type in node_types:
                try:
                    scope.execute(
                        f"MATCH (n:{node_type}) "
                        f"WHERE n.source_session_id = $sid DETACH DELETE n",
                        {"sid": session_id},
                    )
                except Exception:
                    pass

    try:
        run_async_blocking(_run())
    except Exception as exc:
        # Spec 818748f2 — FR4 + BR4: downgrade to warning. The compensation
        # failure is recoverable (lock contention, schema drift) and the
        # message must NOT instruct the operator to delete graph.kuzu —
        # use migrate-schema instead.
        logger.warning(
            "kg.compensate_sync.failed session=%s board=%s err=%s. "
            "Schema migration may be needed — run "
            "`python -m okto_pulse.tools.kg_migrate_schema --board %s` "
            "or call MCP tool `okto_pulse_kg_migrate_schema`. "
            "Do NOT delete graph.kuzu (destructive).",
            session_id, board_id, exc, board_id,
            extra={
                "event": "kg.compensate_sync.failed",
                "session_id": session_id,
                "board_id": board_id,
                "remediation": "migrate-schema",
            },
        )


def _connectivity_writer_path(agent_id: str) -> str:
    """Map the runtime caller to the guard's stable writer path vocabulary."""
    if (agent_id or "").startswith("system:"):
        return "deterministic_worker"
    return "commit_consolidation"


def _validate_kuzu_connectivity_before_commit(
    *,
    kconn,
    board_id: str,
    session_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    effective_hints: dict,
    writer_path: str,
    kg_health_state: str,
) -> dict:
    """Validate zero-orphan invariants before any Kùzu mutation.

    The primitives commit path has three ways to report success without a
    fresh create: NOOP, UPDATE target reuse, and natural
    source_artifact_ref dedup/MERGE. All three must prove an existing minimal
    edge or materialize one from the current batch before counters move.
    """
    if not node_candidates:
        return _connectivity_empty_result()

    registry = KGConnectivityRuleRegistry()
    guard = KGNodeConnectivityGuard(
        registry,
        metric_sink=_connectivity_metric_sink(),
    )
    _auto_attach_provenance_edges(
        kconn=kconn,
        node_candidates=node_candidates,
        edge_candidates=edge_candidates,
    )
    guard_nodes: list[object] = []
    guard_edges: list[object] = list(edge_candidates.values())
    existing_refs: list[KGNodeRef] = []
    candidate_to_existing_id: dict[str, str] = {}

    for cand_id, cand in node_candidates.items():
        node_type = _enum_value(cand.node_type)
        op = _resolve_op(effective_hints.get(cand_id), cand.source_confidence)
        existing_id = _planned_existing_node_id(
            kconn=kconn,
            cand=cand,
            node_type=node_type,
            op=op,
            hint=effective_hints.get(cand_id),
        )
        if op == ReconciliationOperation.NOOP and not existing_id:
            continue

        guard_nodes.append(cand)
        if existing_id:
            candidate_to_existing_id[cand_id] = existing_id
            existing_refs.extend(
                _existing_refs_for_candidate(
                    candidate_id=cand_id,
                    node_type=node_type,
                    node_id=existing_id,
                    source_artifact_ref=cand.source_artifact_ref,
                )
            )
            guard_edges.extend(
                _existing_connectivity_edges_for_candidate(
                    kconn=kconn,
                    registry=registry,
                    candidate_id=cand_id,
                    cand=cand,
                    node_type=node_type,
                    node_id=existing_id,
                    existing_refs=existing_refs,
                )
            )

        if (
            op == ReconciliationOperation.SUPERSEDE
            and effective_hints.get(cand_id)
            and getattr(effective_hints[cand_id], "target_node_id", None)
        ):
            target_id = effective_hints[cand_id].target_node_id
            target_type = _lookup_node_type_by_id(kconn, target_id) or node_type
            guard_edges.append({
                "candidate_id": f"{cand_id}__supersedes_existing",
                "edge_type": "supersedes",
                "from_candidate_id": cand_id,
                "to_candidate_id": f"kg:{target_id}",
            })
            existing_refs.append(
                KGNodeRef(
                    ref_id=f"kg:{target_id}",
                    node_type=target_type,
                    source_artifact_ref=None,
                )
            )

    existing_refs.extend(
        _existing_refs_for_edge_endpoints(
            kconn=kconn,
            edge_candidates=edge_candidates,
            node_candidates=node_candidates,
            candidate_to_existing_id=candidate_to_existing_id,
        )
    )

    result = guard.validate(
        board_id=board_id,
        writer_path=writer_path,
        kg_health_state=kg_health_state,
        nodes=guard_nodes,
        edges=guard_edges,
        existing_node_refs=existing_refs,
        generation_id="",
    )
    response = result.to_response()
    response["checked_nodes"] = len(guard_nodes)
    response["enforced"] = True
    response["kg_health_state"] = kg_health_state
    if result.passed:
        return response

    if kg_health_state in DEGRADED_KG_STATES:
        raise KGPrimitiveError(
            KG_GRAPH_DEGRADED_ERROR_CODE,
            (
                "KG graph is degraded; connectivity validation was deferred "
                "and no graph mutation was attempted."
            ),
            session_id=session_id,
            details={
                "connectivity": response,
                "kg_health_state": kg_health_state,
            },
        )

    details: dict = {"connectivity": response}
    # R7: a working-only canonical Learning bug-derived candidate is an EXPECTED
    # semantic hold, not a generic orphan. Attach a bounded hold payload so the
    # async worker can materialize the go-forward hold in the
    # CognitiveConsolidationItemStore (never CanonicalDebt/DLQ). The guard stays
    # a pure policy function: it does not touch the store/db here.
    r7_hold = _r7_cognitive_hold_payload(result, session_id=session_id)
    if r7_hold is not None:
        details["r7_cognitive_hold_candidate"] = r7_hold
    raise KGPrimitiveError(
        CONNECTIVITY_ERROR_CODE,
        "KG node connectivity guard rejected the commit before graph mutation.",
        session_id=session_id,
        details=details,
    )


def _infer_artifact_type_from_source_ref(source_ref: str | None) -> str | None:
    """Best-effort, bounded artifact_type from a source_artifact_ref.

    bug:* / card:bug:* / *:bug:* → "bug" (the bug_learning hold is always
    bug-derived); otherwise the leading known-type prefix, else None. The worker
    re-derives the store-acceptable type, so this is only a hint.
    """
    s = source_ref or ""
    if s.startswith("bug:") or s.startswith("card:bug:") or ":bug:" in s:
        return "bug"
    if ":" in s:
        prefix = s.split(":", 1)[0]
        if prefix in {
            "spec",
            "decision",
            "refinement",
            "task",
            "test",
            "bug",
            "card",
            "story",
            "ideation",
            "sprint",
        }:
            return prefix
    return None


def _r7_cognitive_hold_payload(result, *, session_id: str) -> dict | None:
    """Build the bounded r7_cognitive_hold_candidate payload from the first
    working-only canonical Learning violation, or None when no such violation
    is present. No content/PII — only refs, ids and layer descriptors."""
    for violation in result.violations:
        if violation.reason == CANONICAL_LEARNING_WORKING_ONLY_REASON:
            source_ref = violation.source_artifact_ref or ""
            return {
                "reason_code": CANONICAL_LEARNING_WORKING_ONLY_REASON,
                "node_type": violation.node_type,
                "candidate_id": violation.candidate_id,
                "source_ref": source_ref,
                "artifact_type": _infer_artifact_type_from_source_ref(source_ref),
                "observed_endpoints": list(violation.observed_endpoints),
                "session_id": session_id,
            }
    return None


def _auto_attach_provenance_edges(
    *,
    kconn,
    node_candidates: dict,
    edge_candidates: dict,
) -> None:
    """Materialize deterministic provenance for cognitive nodes when resolvable.

    Agents are intentionally not allowed to emit ``belongs_to`` edges. When a
    cognitive candidate's source ref resolves to an existing root Entity/Bug, the
    commit path owns that deterministic edge so the connectivity guard has a
    valid, auditable remediation path instead of asking the agent to do something
    the API rejects.
    """
    for cand_id, cand in list(node_candidates.items()):
        if _has_outgoing_edge(edge_candidates, cand_id, "belongs_to"):
            continue
        source_ref = str(getattr(cand, "source_artifact_ref", "") or "")
        if not source_ref:
            continue
        root = _resolve_provenance_root(kconn, source_ref)
        if root is None:
            continue
        root_node_id, root_node_type = root
        cand_node_type = _enum_value(getattr(cand, "node_type", ""))
        if (cand_node_type, root_node_type) not in _allowed_edge_pairs("belongs_to"):
            continue
        edge_id = f"{cand_id}__auto_belongs_to_source_root"
        if edge_id in edge_candidates:
            continue
        edge_candidates[edge_id] = EdgeCandidate(
            candidate_id=edge_id,
            edge_type=KGEdgeType.BELONGS_TO,
            from_candidate_id=cand_id,
            to_candidate_id=f"kg:{root_node_id}",
            confidence=1.0,
            layer="deterministic",
            rule_id="belongs_to/auto_source_root@commit_consolidation",
            created_by="system:commit_consolidation",
            fallback_reason=(
                "auto_attached_to_"
                f"{root_node_type.lower()}_source_root"
            ),
        )


def _has_outgoing_edge(edge_candidates: dict, cand_id: str, edge_type: str) -> bool:
    for edge in edge_candidates.values():
        if (
            str(getattr(edge, "from_candidate_id", "")) == cand_id
            and _enum_value(getattr(edge, "edge_type", "")) == edge_type
        ):
            return True
    return False


def _resolve_provenance_root(kconn, source_ref: str) -> tuple[str, str] | None:
    for root_ref in _source_root_ref_candidates(source_ref):
        for node_type in ("Entity", "Bug"):
            node_id = _lookup_existing_node(kconn, node_type, root_ref)
            if node_id:
                return node_id, node_type
    return None


def _source_root_ref_candidates(source_ref: str) -> tuple[str, ...]:
    parts = [part for part in source_ref.split(":") if part]
    candidates: list[str] = []
    if len(parts) >= 2:
        kind = parts[0]
        if kind in {"spec", "refinement", "ideation", "story"}:
            candidates.append(f"{kind}:{parts[1]}")
        elif kind == "bug":
            candidates.append(f"bug:{parts[1]}")
            candidates.append(f"card:{parts[1]}")
        elif kind == "card":
            if len(parts) >= 3 and parts[1] == "bug":
                candidates.append(f"bug:{parts[2]}")
                candidates.append(f"card:{parts[2]}")
                candidates.append(f"card:bug:{parts[2]}")
            else:
                candidates.append(f"card:{parts[1]}")
    candidates.append(source_ref)
    return tuple(dict.fromkeys(candidates))


def _validate_degraded_connectivity_before_open(
    *,
    board_id: str,
    session_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    writer_path: str,
    kg_health_state: str,
) -> None:
    """Return a contextual error before opening a degraded graph."""
    if not node_candidates or kg_health_state not in DEGRADED_KG_STATES:
        return

    registry = KGConnectivityRuleRegistry()
    guard = KGNodeConnectivityGuard(
        registry,
        metric_sink=_connectivity_metric_sink(),
    )
    result = guard.validate(
        board_id=board_id,
        writer_path=writer_path,
        kg_health_state=kg_health_state,
        nodes=list(node_candidates.values()),
        edges=list(edge_candidates.values()),
        existing_node_refs=(),
        generation_id="",
    )
    response = result.to_response()
    response["checked_nodes"] = len(node_candidates)
    response["enforced"] = True
    response["kg_health_state"] = kg_health_state
    raise KGPrimitiveError(
        KG_GRAPH_DEGRADED_ERROR_CODE,
        (
            "KG graph is degraded; connectivity validation was deferred "
            "and no graph mutation was attempted."
        ),
        session_id=session_id,
        details={
            "connectivity": response,
            "kg_health_state": kg_health_state,
        },
    )


def _connectivity_empty_result() -> dict:
    return {
        "passed": True,
        "violations": [],
        "allowlisted_roots": [],
        "materializable_edges": [],
        "outcome": "passed",
        "checked_nodes": 0,
        "enforced": True,
    }


def _planned_existing_node_id(
    *,
    kconn,
    cand,
    node_type: str,
    op: ReconciliationOperation,
    hint: ReconciliationHint | None,
) -> str | None:
    source_ref = cand.source_artifact_ref or ""
    if source_ref:
        existing_by_source = _lookup_existing_node(kconn, node_type, source_ref)
        if existing_by_source:
            return existing_by_source

    if op == ReconciliationOperation.NOOP:
        return None

    target_node_id = getattr(hint, "target_node_id", None) if hint else None
    if op == ReconciliationOperation.UPDATE and target_node_id:
        is_curated = _node_is_human_curated(kconn, node_type, target_node_id)
        has_override = bool(hint.confidence and hint.confidence >= 1.0)
        if is_curated and has_override:
            return None
        return target_node_id

    return None


def _existing_refs_for_candidate(
    *,
    candidate_id: str,
    node_type: str,
    node_id: str,
    source_artifact_ref: str | None,
) -> list[KGNodeRef]:
    refs = [
        KGNodeRef(
            ref_id=candidate_id,
            node_type=node_type,
            source_artifact_ref=source_artifact_ref,
        ),
        KGNodeRef(
            ref_id=node_id,
            node_type=node_type,
            source_artifact_ref=source_artifact_ref,
        ),
        KGNodeRef(
            ref_id=f"kg:{node_id}",
            node_type=node_type,
            source_artifact_ref=source_artifact_ref,
        ),
    ]
    if source_artifact_ref:
        refs.append(
            KGNodeRef(
                ref_id=source_artifact_ref,
                node_type=node_type,
                source_artifact_ref=source_artifact_ref,
            )
        )
    return refs


def _existing_refs_for_edge_endpoints(
    *,
    kconn,
    edge_candidates: dict,
    node_candidates: dict,
    candidate_to_existing_id: dict[str, str],
) -> list[KGNodeRef]:
    refs: list[KGNodeRef] = []
    for edge in edge_candidates.values():
        for endpoint in (edge.from_candidate_id, edge.to_candidate_id):
            if endpoint in node_candidates:
                continue
            node_id, resolved_type = _resolve_endpoint(
                endpoint, candidate_to_existing_id, kconn=kconn,
            )
            if not node_id:
                continue
            node_type = resolved_type or _lookup_node_type_by_id(kconn, node_id)
            if not node_type:
                continue
            # R7: carry the endpoint's graph_layer so the layer-aware guard can
            # tell a canonical Bug from a working one for explicit edge endpoints.
            node_layer = _lookup_node_layer_by_id(kconn, node_type, node_id)
            # RKG-02: also carry the real source_artifact_ref so the guard's
            # type-aware canonical-bug probe can reconcile a Learning's
            # card:<uuid> with this existing Bug endpoint.
            node_source_ref = _lookup_node_source_ref_by_id(kconn, node_type, node_id)
            refs.append(
                KGNodeRef(ref_id=endpoint, node_type=node_type, graph_layer=node_layer,
                          source_artifact_ref=node_source_ref)
            )
            refs.append(
                KGNodeRef(ref_id=node_id, node_type=node_type, graph_layer=node_layer,
                          source_artifact_ref=node_source_ref)
            )
            refs.append(
                KGNodeRef(
                    ref_id=f"kg:{node_id}", node_type=node_type, graph_layer=node_layer,
                    source_artifact_ref=node_source_ref,
                )
            )
    return refs


def _existing_connectivity_edges_for_candidate(
    *,
    kconn,
    registry: KGConnectivityRuleRegistry,
    candidate_id: str,
    cand,
    node_type: str,
    node_id: str,
    existing_refs: list[KGNodeRef],
) -> list[dict[str, str]]:
    try:
        rule = registry.get_rule(node_type)
    except KeyError:
        return []

    bug_probe = _graph_canonical_bug_probe(kconn)
    if node_type == "Learning" and _candidate_has_known_bug_source(cand, bug_probe):
        from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL

        # Mirrors connectivity_guard._learning_bug_group: a bug-derived canonical
        # Learning only reaches completeness through a *canonical* Bug (R7).
        groups = [
            KGConnectivityEdgeGroup(
                name="bug_learning",
                alternatives=(
                    KGConnectivityEdgeRequirement(
                        "validates", "outgoing", ("Bug",),
                        required_target_layer=GRAPH_LAYER_CANONICAL,
                    ),
                ),
                remediation_hint=(
                    "Provide Learning -> validates -> canonical Bug when the "
                    "learning is derived from a known bug."
                ),
            )
        ]
    else:
        groups = list(rule.required_edge_groups)

    synthesized: list[dict[str, str]] = []
    for group in groups:
        for req in group.alternatives:
            match = None
            if req.required_target_layer is not None:
                # R7: prefer a canonical endpoint for completeness; only fall
                # back to ANY existing endpoint so a working-only existing edge
                # is still surfaced to the guard (its real layer rides the ref).
                match = _find_existing_connectivity_match(
                    kconn=kconn,
                    node_type=node_type,
                    node_id=node_id,
                    edge_type=req.edge_type,
                    direction=req.direction,
                    target_node_types=req.target_node_types,
                    required_target_layer=req.required_target_layer,
                )
            if match is None:
                match = _find_existing_connectivity_match(
                    kconn=kconn,
                    node_type=node_type,
                    node_id=node_id,
                    edge_type=req.edge_type,
                    direction=req.direction,
                    target_node_types=req.target_node_types,
                )
            if match is None:
                continue
            other_id, other_type, direction, other_layer = match
            endpoint_ref = f"kg:{other_id}"
            # RKG-02: carry the matched endpoint's real source_artifact_ref so the
            # guard's canonical-bug probe can reconcile card:<uuid> with the Bug.
            other_source_ref = _lookup_node_source_ref_by_id(kconn, other_type, other_id)
            existing_refs.append(
                KGNodeRef(
                    ref_id=endpoint_ref,
                    node_type=other_type,
                    graph_layer=other_layer,
                    source_artifact_ref=other_source_ref,
                )
            )
            existing_refs.append(
                KGNodeRef(ref_id=other_id, node_type=other_type, graph_layer=other_layer,
                          source_artifact_ref=other_source_ref)
            )
            if direction == "outgoing":
                from_ref = candidate_id
                to_ref = endpoint_ref
            else:
                from_ref = endpoint_ref
                to_ref = candidate_id
            synthesized.append({
                "candidate_id": f"{candidate_id}__existing_{req.edge_type}",
                "edge_type": req.edge_type,
                "from_candidate_id": from_ref,
                "to_candidate_id": to_ref,
            })
            break
    return synthesized


def _candidate_has_known_bug_source(cand, bug_probe=None) -> bool:
    """Bug-derived detection for a Learning candidate via the shared resolver
    (RKG-02 / BR3 — no divergent local parser). Explicit bug fields and
    bug:/card:bug: forms are always bug-derived; a plain card:<uuid> is
    bug-derived only when ``bug_probe`` confirms a canonical Bug."""
    for attr in (
        "bug_id",
        "bug_ref",
        "known_bug_ref",
        "known_bug_source_ref",
        "target_bug_ref",
    ):
        if getattr(cand, attr, None):
            return True
    from okto_pulse.core.kg.cognitive_source_ref_resolver import resolve_cognitive_source_ref

    source_ref = getattr(cand, "source_artifact_ref", "") or ""
    return resolve_cognitive_source_ref(source_ref, canonical_bug_probe=bug_probe).is_bug_derived


def _find_existing_connectivity_match(
    *,
    kconn,
    node_type: str,
    node_id: str,
    edge_type: str,
    direction: str,
    target_node_types: tuple[str, ...],
    required_target_layer: str | None = None,
) -> tuple[str, str, str, str | None] | None:
    """Find an existing connectivity edge for ``node_id``.

    Returns ``(other_id, other_type, direction, other_graph_layer)`` or None.
    When ``required_target_layer`` is set the match is restricted to endpoints
    on that graph_layer (R7: prefer a canonical Bug for canonical Learning
    completeness); a NULL layer therefore never matches a canonical filter.
    """
    directions = ("outgoing", "incoming") if direction == "any" else (direction,)
    targets = target_node_types or tuple(_kg_node_types())
    params: dict[str, str] = {"node_id": node_id}
    layer_clause = ""
    if required_target_layer is not None:
        layer_clause = " AND m.graph_layer = $layer"
        params["layer"] = required_target_layer
    for actual_direction in directions:
        for target_type in targets:
            if actual_direction == "outgoing":
                cypher = (
                    f"MATCH (n:{node_type})-[r:{edge_type}]->(m:{target_type}) "
                    f"WHERE n.id = $node_id{layer_clause} "
                    "RETURN m.id, m.graph_layer LIMIT 1"
                )
            else:
                cypher = (
                    f"MATCH (m:{target_type})-[r:{edge_type}]->(n:{node_type}) "
                    f"WHERE n.id = $node_id{layer_clause} "
                    "RETURN m.id, m.graph_layer LIMIT 1"
                )
            try:
                res = kconn.execute(cypher, params)
                try:
                    if res.has_next():
                        row = res.get_next()
                        return row[0], target_type, actual_direction, row[1]
                finally:
                    try:
                        res.close()
                    except Exception:
                        pass
            except Exception:
                continue
    return None


def _lookup_node_layer_by_id(kconn, node_type: str, node_id: str) -> str | None:
    """Return a node's graph_layer (or None if unknown/unreadable).

    None is fail-closed downstream: the R7 layer-aware guard treats an
    unresolved layer as non-canonical.
    """
    if not node_id or not node_type:
        return None
    try:
        res = kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.graph_layer LIMIT 1",
            {"id": node_id},
        )
        try:
            if res.has_next():
                value = res.get_next()[0]
                return str(value) if value is not None else None
        finally:
            try:
                res.close()
            except Exception:
                pass
    except Exception:
        return None
    return None


def _lookup_node_source_ref_by_id(kconn, node_type: str, node_id: str) -> str | None:
    """Return a node's source_artifact_ref (or None). RKG-02: the connectivity
    guard's type-aware canonical-bug probe needs the real Bug source_ref of an
    EXISTING endpoint to reconcile a Learning's card:<uuid> with the canonical
    Bug; the worker must load it instead of leaving it None."""
    if not node_id or not node_type:
        return None
    try:
        res = kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.source_artifact_ref LIMIT 1",
            {"id": node_id},
        )
        try:
            if res.has_next():
                value = res.get_next()[0]
                return str(value) if value is not None else None
        finally:
            try:
                res.close()
            except Exception:
                pass
    except Exception:
        return None
    return None


def _graph_canonical_bug_probe(kconn):
    """A type-aware canonical-bug probe (RKG-02 / FR2) backed by the live graph:
    ``probe(uuid)`` is True iff a *canonical* Bug exists whose id or
    source_artifact_ref reconciles to ``card:<uuid>``. Fail-closed: any read
    error or non-canonical Bug yields False."""
    from okto_pulse.core.kg.cognitive_source_ref_resolver import strip_concept_suffix
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    keys: set[str] = set()
    loaded = False

    def _ensure_loaded() -> None:
        nonlocal loaded
        if loaded:
            return
        loaded = True
        try:
            res = kconn.execute(
                "MATCH (b:Bug) WHERE b.graph_layer = 'canonical' "
                "RETURN b.id, b.source_artifact_ref",
                {},
            )
            try:
                while res.has_next():
                    bid, bsref = res.get_next()
                    if bid:
                        keys.add(normalize_cognitive_artifact_id(f"card:{bid}"))
                    if bsref:
                        keys.add(normalize_cognitive_artifact_id(strip_concept_suffix(str(bsref))))
            finally:
                try:
                    res.close()
                except Exception:
                    pass
        except Exception:
            pass

    def _probe(uuid: str) -> bool:
        _ensure_loaded()
        return normalize_cognitive_artifact_id(f"card:{uuid}") in keys

    return _probe


def _lookup_node_type_by_id(kconn, node_id: str) -> str | None:
    if not node_id:
        return None
    for node_type in _kg_node_types():
        try:
            res = kconn.execute(
                f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.id LIMIT 1",
                {"id": node_id},
            )
            try:
                if res.has_next():
                    return node_type
            finally:
                try:
                    res.close()
                except Exception:
                    pass
        except Exception:
            continue
    return None


def _kg_node_types() -> tuple[str, ...]:
    from okto_pulse.core.kg.schema_contract import NODE_TYPES

    return tuple(NODE_TYPES)


# Estado efetivo de health por board para o write-path, com TTL curto.
# `get_kg_health` é caro (dezenas de queries + probes de arquivo); sem cache,
# um batch de N entries do mesmo board recomputa N healths idênticos — parte
# do bloqueio de event loop observado em campo (py-spy 2026-06-10).
_COMMIT_HEALTH_CACHE: dict[str, tuple[float, str]] = {}
_COMMIT_HEALTH_CACHE_TTL_S = 5.0

# Estado sintético devolvido quando o board está recovery_needed mas o grafo
# é LEGÍVEL (o health acabou de contar os nodes): NÃO pertence a
# DEGRADED_KG_STATES, então o gate deixa a mutação passar.
RECOVERY_WRITABLE_STATE = "recovery_needed_graph_readable"
# Alias mantido para os primeiros consumidores do fix (mesma semântica).
RECOVERY_EMPTY_REMATERIALIZATION_STATE = RECOVERY_WRITABLE_STATE


def reset_commit_health_cache_for_tests(board_id: str | None = None) -> None:
    """Test helper — drop the write-path health cache (all boards or one)."""
    if board_id is None:
        _COMMIT_HEALTH_CACHE.clear()
    else:
        _COMMIT_HEALTH_CACHE.pop(board_id, None)


async def _resolve_commit_kg_health_state(board_id: str, db) -> str:
    """Read the write-path KG health state without making tests DB-coupled.

    Catch-22 fix (2026-06-10): ``recovery_needed`` bloqueava TODA mutação —
    inclusive a re-materialização de um grafo vazio (única cura de
    ``empty_after_materialized_history``; 994 entries foram para a DLQ em
    campo) e os retries após falha TRANSITÓRIA de escrita (ex.: buffer
    manager exausto), que re-registrava ``kg.commit.failed`` e realimentava o
    próprio estado. Falha recente de escrita não implica grafo corrompido.

    Regra efetiva para o WRITE-PATH:
    - ``quarantined`` → bloqueia sempre.
    - ``recovery_needed`` com grafo LEGÍVEL (o health acabou de contar os
      nodes — ``total_nodes`` presente, 0 ou N) → permite a mutação: a
      conectividade real é validada contra o grafo aberto, e um commit
      bem-sucedido limpa as falhas de write do ring buffer (self-heal).
    - ``recovery_needed`` SEM contagem (telemetria indisponível = grafo
      ilegível) → mantém o bloqueio/deferral (contrato Zero-Orphan); e
      corrupção real continua fail-closed na própria abertura
      (``throw_on_wal_replay_failure=True``), sem mutação.
    """
    if db is None:
        return "healthy"

    import time as _time

    now = _time.monotonic()
    cached = _COMMIT_HEALTH_CACHE.get(board_id)
    if cached is not None and now - cached[0] < _COMMIT_HEALTH_CACHE_TTL_S:
        return cached[1]

    try:
        from okto_pulse.core.services.kg_health_service import get_kg_health

        health = await get_kg_health(board_id, db)
    except Exception as exc:
        logger.warning(
            "kg.connectivity.health_resolution_failed board=%s err=%s",
            board_id,
            exc,
            extra={
                "event": "kg.connectivity.health_resolution_failed",
                "board_id": board_id,
            },
        )
        return "healthy"
    state = str(health.get("overall_state") or health.get("graph_state") or "healthy")

    raw_total_nodes = health.get("total_nodes")
    if state == "recovery_needed" and raw_total_nodes is not None:
        # total_nodes AUSENTE = payload sem telemetria de contagem (grafo
        # ilegível) → mantém o bloqueio (conservador, contrato Zero-Orphan).
        try:
            total_nodes = int(raw_total_nodes)
        except (TypeError, ValueError):
            total_nodes = -1
        if total_nodes >= 0:
            logger.info(
                "kg.connectivity.recovery_writable_graph board=%s "
                "total_nodes=%d reason=%s",
                board_id,
                total_nodes,
                health.get("classification_reason"),
                extra={
                    "event": "kg.connectivity.recovery_writable_graph",
                    "board_id": board_id,
                    "total_nodes": total_nodes,
                },
            )
            state = RECOVERY_WRITABLE_STATE

    _COMMIT_HEALTH_CACHE[board_id] = (now, state)
    return state


def _do_kuzu_commit(
    board_id: str,
    session_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    effective_hints: dict,
    agent_id: str,
    embedder,
    kg_health_state: str,
) -> tuple[dict, object, list, datetime, dict]:
    """Synchronous Kùzu writes for ``commit_consolidation``.

    Runs in the thread pool via ``_run_kuzu``. Returns
    ``(candidate_to_kuzu_id, counters, records, committed_at, connectivity)``
    on success.
    Raises ``KGPrimitiveError`` on failure (after inline compensation).
    """
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    writer_path = _connectivity_writer_path(agent_id)
    _validate_degraded_connectivity_before_open(
        board_id=board_id,
        session_id=session_id,
        node_candidates=node_candidates,
        edge_candidates=edge_candidates,
        writer_path=writer_path,
        kg_health_state=kg_health_state,
    )

    kconn = run_async_blocking(
        get_kg_registry().graph_transaction.begin(board_id)
    )
    orch = TransactionOrchestrator(
        kuzu_conn=kconn,
        sqlite_session=None,  # SQLite writes happen in async context
        session_id=session_id,
        board_id=board_id,
    )
    candidate_to_kuzu_id: dict[str, str] = {}
    candidate_to_node_type: dict[str, str] = {}

    try:
        connectivity = _validate_kuzu_connectivity_before_commit(
            kconn=kconn,
            board_id=board_id,
            session_id=session_id,
            node_candidates=node_candidates,
            edge_candidates=edge_candidates,
            effective_hints=effective_hints,
            writer_path=writer_path,
            kg_health_state=kg_health_state,
        )
        for cand_id, cand in node_candidates.items():
            hint = effective_hints.get(cand_id)
            op = _resolve_op(hint, cand.source_confidence)
            node_type = _enum_value(cand.node_type)

            if op == ReconciliationOperation.NOOP:
                # Spec eca49df9 (FR6): NOOP is a processed candidate too.
                orch.counters.nodes_noop += 1
                existing_id = _lookup_existing_node(
                    kconn, node_type, cand.source_artifact_ref or ""
                )
                if existing_id:
                    candidate_to_kuzu_id[cand_id] = existing_id
                    candidate_to_node_type[cand_id] = node_type
                continue

            # Spec 4007e4a3 (Ideação #3, BR4 + BR5): UPDATE path must
            # preserve nodes that a human curator has explicitly marked
            # as human_curated. Without an explicit override (extension
            # point — currently signalled by hint.confidence >= 1.0 from
            # an agent-supplied override), the UPDATE is converted to
            # NOOP-with-mapping so downstream edges still resolve. With
            # an override, the agent reasserts ownership and the new
            # node defaults to human_curated=False (BR5 reset).
            if op == ReconciliationOperation.UPDATE and hint and getattr(hint, "target_node_id", None):
                target_node_id = hint.target_node_id
                node_type_check = _enum_value(cand.node_type)
                is_curated = _node_is_human_curated(kconn, node_type_check, target_node_id)
                has_override = bool(hint.confidence and hint.confidence >= 1.0)
                if is_curated and not has_override:
                    logger.info(
                        "kg.consolidation.manual_edit_preserved candidate=%s "
                        "target_node_id=%s session=%s",
                        cand_id, target_node_id, session_id,
                        extra={
                            "event": "kg.consolidation.manual_edit_preserved",
                            "candidate_id": cand_id,
                            "target_node_id": target_node_id,
                            "session_id": session_id,
                        },
                    )
                    candidate_to_kuzu_id[cand_id] = target_node_id
                    candidate_to_node_type[cand_id] = node_type_check
                    continue
                if is_curated and has_override:
                    # BR5 reset: agent reclaims authorship via override.
                    # The downstream create_node already initialises
                    # human_curated=False, so the reset is implicit. We
                    # just emit the counter so observability tooling can
                    # distinguish overrides from green-field UPDATEs.
                    logger.info(
                        "kg.consolidation.reset_manual_flag candidate=%s "
                        "target_node_id=%s session=%s",
                        cand_id, target_node_id, session_id,
                        extra={
                            "event": "kg.consolidation.reset_manual_flag",
                            "candidate_id": cand_id,
                            "target_node_id": target_node_id,
                            "session_id": session_id,
                        },
                    )

                if not is_curated:
                    # Spec eca49df9 (FR6/TR6): a non-curated UPDATE with a
                    # target is an in-place semantic update — route it
                    # through orch.update_node so nodes_updated has a real
                    # production call site. The NC-8 dedup-reuse branch
                    # below stays a MERGE; an UPDATE must not be miscounted
                    # as MERGE or CREATE. (Curated+override keeps falling
                    # through to reset-via-create.)
                    update_attrs = {
                        "title": cand.title,
                        "content": cand.content or "",
                        "context": cand.context or "",
                        "justification": cand.justification or "",
                        "graph_layer": getattr(cand, "graph_layer", "canonical"),
                        "maturity_status": getattr(
                            cand, "maturity_status", "canonical_eligible"
                        ),
                        "source_confidence": cand.source_confidence,
                        "priority_boost": getattr(cand, "priority_boost", 0.0),
                    }
                    orch.update_node(node_type_check, target_node_id, update_attrs)
                    candidate_to_kuzu_id[cand_id] = target_node_id
                    candidate_to_node_type[cand_id] = node_type_check
                    logger.info(
                        "kg.consolidation.updated candidate=%s target=%s "
                        "type=%s session=%s",
                        cand_id, target_node_id, node_type_check, session_id,
                        extra={
                            "event": "kg.consolidation.updated",
                            "candidate_id": cand_id,
                            "target_node_id": target_node_id,
                            "node_type": node_type_check,
                            "session_id": session_id,
                        },
                    )
                    continue

            # Spec eca49df9 (FR4): op==SUPERSEDE must go through
            # supersede_node (new node + superseded_by + :supersedes edge
            # for node types the schema supports — Decision today), never
            # fall through to a lone CREATE. supersede_node reclassifies the
            # counter (nodes_added-1 / nodes_superseded+1) internally.
            if (
                op == ReconciliationOperation.SUPERSEDE
                and hint
                and getattr(hint, "target_node_id", None)
            ):
                superseded_id = hint.target_node_id
                new_node_id = f"{node_type.lower()}_{uuid.uuid4().hex[:12]}"
                embedding = embedder.encode(f"{cand.title}\n{cand.content or ''}")
                new_attrs = {
                    "title": cand.title,
                    "content": cand.content or "",
                    "context": cand.context or "",
                    "justification": cand.justification or "",
                    "source_artifact_ref": cand.source_artifact_ref or "",
                    "graph_layer": getattr(cand, "graph_layer", "canonical"),
                    "maturity_status": getattr(
                        cand, "maturity_status", "canonical_eligible"
                    ),
                    "created_at": _now_iso(),
                    "created_by_agent": agent_id,
                    "source_confidence": cand.source_confidence,
                    "relevance_score": getattr(cand, "relevance_score", 0.5),
                    "query_hits": 0,
                    "last_queried_at": None,
                    "priority_boost": getattr(cand, "priority_boost", 0.0),
                    "human_curated": False,
                    "embedding": embedding,
                }
                orch.supersede_node(
                    node_type, new_node_id, superseded_id, new_attrs,
                    revocation_reason="superseded by consolidation session",
                )
                candidate_to_kuzu_id[cand_id] = new_node_id
                candidate_to_node_type[cand_id] = node_type
                logger.info(
                    "kg.consolidation.superseded candidate=%s new=%s old=%s "
                    "type=%s session=%s",
                    cand_id, new_node_id, superseded_id, node_type, session_id,
                    extra={
                        "event": "kg.consolidation.superseded",
                        "candidate_id": cand_id,
                        "new_node_id": new_node_id,
                        "superseded_node_id": superseded_id,
                        "node_type": node_type,
                        "session_id": session_id,
                    },
                )
                continue

            # Spec 7f23535f (NC-8): natural dedup by source_artifact_ref.
            # Before generating a fresh UUID, check whether this artifact
            # already has a Kuzu node from a prior session. If yes, reuse
            # it (UPDATE attrs unless human_curated). Without this branch
            # every spec.semantic_changed / spec.moved / spec.version_bumped
            # event spawns a duplicate Entity for the same source.
            source_ref = cand.source_artifact_ref or ""
            if source_ref:
                existing_id = _lookup_existing_node(
                    kconn, node_type, source_ref
                )
                if existing_id:
                    is_curated = _node_is_human_curated(
                        kconn, node_type, existing_id
                    )
                    if not is_curated:
                        embedding = embedder.encode(
                            f"{cand.title}\n{cand.content or ''}"
                        )
                        update_attrs = {
                            "title": cand.title,
                            "content": cand.content or "",
                            "context": cand.context or "",
                            "justification": cand.justification or "",
                            "graph_layer": getattr(cand, "graph_layer", "canonical"),
                            "maturity_status": getattr(
                                cand, "maturity_status", "canonical_eligible"
                            ),
                            "source_confidence": cand.source_confidence,
                            "priority_boost": getattr(cand, "priority_boost", 0.0),
                            "embedding": embedding,
                        }
                        _apply_kuzu_node_update_partial(
                            orch, node_type, existing_id, update_attrs
                        )
                    else:
                        # FR4 / dec_85ba8dc2 (card 302044a7): a human_curated
                        # node keeps its PROTECTED content (title/content/
                        # context/justification/embedding/source_confidence),
                        # but maturity METADATA still promotes — a working
                        # node a human curated must still reach canonical when
                        # its source spec becomes done. Update ONLY the
                        # maturity metadata (partial), never the curated
                        # content.
                        _apply_kuzu_node_update_partial(
                            orch,
                            node_type,
                            existing_id,
                            {
                                "graph_layer": getattr(
                                    cand, "graph_layer", "canonical"
                                ),
                                "maturity_status": getattr(
                                    cand, "maturity_status", "canonical_eligible"
                                ),
                            },
                        )
                    candidate_to_kuzu_id[cand_id] = existing_id
                    candidate_to_node_type[cand_id] = node_type
                    # Spec eca49df9 (FR5/AC6): count + audit the NC-8
                    # dedup-reuse (merge). No KuzuWriteRecord — the reused
                    # node belongs to a prior session and must not enter
                    # compensation rollback.
                    orch.counters.nodes_merged += 1
                    orch.counters.merge_audit_items.append({
                        "candidate_id": cand_id,
                        "node_type": node_type,
                        "source_artifact_ref": source_ref,
                        "reused_node_id": existing_id,
                        "operation": "MERGE",
                    })
                    logger.info(
                        "kg.consolidation.dedup_reused candidate=%s "
                        "existing=%s type=%s ref=%s session=%s curated=%s",
                        cand_id, existing_id, node_type, source_ref,
                        session_id, is_curated,
                        extra={
                            "event": "kg.consolidation.dedup_reused",
                            "cand_id": cand_id,
                            "existing_id": existing_id,
                            "node_type": node_type,
                            "source_artifact_ref": source_ref,
                            "session_id": session_id,
                            "was_curated_preserved": is_curated,
                        },
                    )
                    continue

            node_id = f"{node_type.lower()}_{uuid.uuid4().hex[:12]}"
            embedding = embedder.encode(f"{cand.title}\n{cand.content or ''}")

            node_attrs = {
                "title": cand.title,
                "content": cand.content or "",
                "context": cand.context or "",
                "justification": cand.justification or "",
                "source_artifact_ref": cand.source_artifact_ref or "",
                "graph_layer": getattr(cand, "graph_layer", "canonical"),
                "maturity_status": getattr(
                    cand, "maturity_status", "canonical_eligible"
                ),
                "created_at": _now_iso(),
                "created_by_agent": agent_id,
                "source_confidence": cand.source_confidence,
                "relevance_score": getattr(cand, "relevance_score", 0.5),
                "query_hits": 0,
                "last_queried_at": None,
                "priority_boost": getattr(cand, "priority_boost", 0.0),
                # Spec 4007e4a3 (Ideação #3): nodes are agent-managed by
                # default. A human curator may set human_curated=TRUE
                # later via back-office; the UPDATE path then skips
                # writes unless the agent passes an explicit override.
                "human_curated": False,
                "embedding": embedding,
            }
            _apply_kuzu_node_create_with_timestamp(
                orch, node_type, node_id, node_attrs
            )
            candidate_to_kuzu_id[cand_id] = node_id
            candidate_to_node_type[cand_id] = node_type

        for edge in edge_candidates.values():
            from_id, from_xref_type = _resolve_endpoint(
                edge.from_candidate_id, candidate_to_kuzu_id, kconn=kconn,
            )
            to_id, to_xref_type = _resolve_endpoint(
                edge.to_candidate_id, candidate_to_kuzu_id, kconn=kconn,
            )
            if from_id is None or to_id is None:
                continue
            edge_attrs: dict[str, object] = {"confidence": edge.confidence}
            if edge.layer:
                edge_attrs["layer"] = edge.layer
            if edge.rule_id:
                edge_attrs["rule_id"] = edge.rule_id
            if edge.created_by:
                edge_attrs["created_by"] = edge.created_by
            if edge.fallback_reason:
                edge_attrs["fallback_reason"] = edge.fallback_reason
            from_cand = node_candidates.get(edge.from_candidate_id)
            to_cand = node_candidates.get(edge.to_candidate_id)
            from_hint = (
                _enum_value(from_cand.node_type) if from_cand
                else from_xref_type
            )
            to_hint = (
                _enum_value(to_cand.node_type) if to_cand
                else to_xref_type
            )
            orch.create_edge(
                edge_type=_enum_value(edge.edge_type),
                from_id=from_id,
                to_id=to_id,
                attrs=edge_attrs,
                from_type=from_hint,
                to_type=to_hint,
            )

        # v0.3.0 R2: recompute relevance_score for every node touched by
        # this session. This includes nodes-only fallback commits, which
        # otherwise stay pinned to the neutral 0.5 score and inflate
        # kg_health.default_score_ratio.
        try:
            from okto_pulse.core.kg.scoring import _recompute_relevance_batch

            endpoints_to_recompute: list[tuple[str, str]] = []
            seen: set[tuple[str, str]] = set()
            for cand_id, node_id in candidate_to_kuzu_id.items():
                node_type = candidate_to_node_type.get(cand_id)
                if not node_type:
                    continue
                key = (node_type, node_id)
                if key not in seen:
                    seen.add(key)
                    endpoints_to_recompute.append(key)
            for edge in edge_candidates.values():
                from_id_resolved, from_type_resolved = _resolve_endpoint(
                    edge.from_candidate_id, candidate_to_kuzu_id, kconn=kconn,
                )
                if from_type_resolved is None:
                    from_type_resolved = candidate_to_node_type.get(
                        edge.from_candidate_id
                    )
                to_id_resolved, to_type_resolved = _resolve_endpoint(
                    edge.to_candidate_id, candidate_to_kuzu_id, kconn=kconn,
                )
                if to_type_resolved is None:
                    to_type_resolved = candidate_to_node_type.get(
                        edge.to_candidate_id
                    )
                if from_id_resolved and from_type_resolved:
                    key = (from_type_resolved, from_id_resolved)
                    if key not in seen:
                        seen.add(key)
                        endpoints_to_recompute.append(key)
                if to_id_resolved and to_type_resolved:
                    key = (to_type_resolved, to_id_resolved)
                    if key not in seen:
                        seen.add(key)
                        endpoints_to_recompute.append(key)
            if endpoints_to_recompute:
                _recompute_relevance_batch(
                    kconn, board_id, endpoints_to_recompute,
                    trigger="degree_delta",
                )
        except Exception as exc:
            logger.warning(
                "kg.scoring.commit_hook_failed session=%s err=%s",
                session_id, exc,
            )

        committed_at = datetime.now(timezone.utc)
        run_async_blocking(kconn.commit())
        return (
            candidate_to_kuzu_id,
            orch.counters,
            list(orch.records),
            committed_at,
            connectivity,
        )

    except KGPrimitiveError:
        try:
            run_async_blocking(kconn.rollback())
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            run_async_blocking(kconn.rollback())
        except Exception:
            pass
        _compensate_kuzu_writes(board_id, session_id, orch.records)
        message, details = _contextualize_ladybug_commit_error(exc)
        raise KGPrimitiveError(
            "commit_failed",
            f"commit failed and was compensated: {message}",
            session_id=session_id,
            details=details,
        ) from exc


async def commit_consolidation(
    req: CommitConsolidationRequest,
    *,
    agent_id: str,
    db=None,
) -> CommitConsolidationResponse:
    """Atomically write Kuzu nodes/edges + audit + outbox event.

    Kùzu writes are offloaded to the thread pool via ``_run_kuzu`` and
    ``_do_kuzu_commit``.  Audit persistence (SQLite) remains in the async
    context via ``_commit_audit_records``.

    KG-01 FR5/FR6 enforcement: this primitive is a write path against
    `graph.lbug` and MUST run inside a `under_safe_write` guard. In SOFT
    mode (default) a missing guard logs `kg.write_barrier.unguarded` and
    bumps `kg_unguarded_write_total` without blocking — production wires
    callers to enter the guard via KGSafeWriteLifecycle. In STRICT mode
    (tests + dedicated production deployments) the absence of a guard
    raises ``WriteLifecycleViolation`` before the Kùzu write begins.
    """
    from okto_pulse.core.kg.write_barrier import require_write_token

    registry = get_kg_registry()
    session = await _require_open_session(req.session_id, agent_id)

    # FR5/FR6 barrier check. Uses session.board_id so a multi-board
    # process never blocks the wrong board. Raises in STRICT, logs+counter
    # in SOFT.
    require_write_token(session.board_id)

    async with session.lock:
        effective_hints = dict(session.reconciliation_hints)
        for cid, override in req.agent_overrides.items():
            effective_hints[cid] = override

        kg_health_state = await _resolve_commit_kg_health_state(
            session.board_id,
            db,
        )

        # --- Kùzu writes (offloaded to thread pool) ---
        try:
            (
                candidate_to_kuzu_id,
                counters,
                records,
                committed_at,
                connectivity,
            ) = await _run_kuzu(
                _do_kuzu_commit,
                session.board_id,
                req.session_id,
                dict(session.node_candidates),
                dict(session.edge_candidates),
                effective_hints,
                agent_id,
                registry.require_embedding_provider(),
                kg_health_state,
            )
        except KGPrimitiveError:
            raise
        except Exception as exc:
            message, details = _contextualize_ladybug_commit_error(exc)
            raise KGPrimitiveError(
                "commit_failed",
                f"Kùzu commit failed: {message}",
                session_id=req.session_id,
                details=details,
            ) from exc

        # --- SQLite audit + outbox (async, remains in event loop) ---
        await _commit_audit_records(
            registry, db, records, counters, req, session, agent_id, committed_at,
        )

        session.status = SessionStatus.COMMITTED
        session.committed_kuzu_node_refs = [
            {"node_id": r.entity_id, "node_type": r.entity_type, "kind": r.kind}
            for r in records
        ]
        await registry.require_session_store().remove(req.session_id)

        registry.require_cache_backend().invalidate_board(session.board_id)

        # Commit bem-sucedido prova que o write-path está saudável:
        # (a) limpa falhas de WAL/commit do ring buffer (sem isso, o health
        #     manteria wal_or_commit_errors até o restart — feedback loop);
        # (b) invalida o cache de health do write-path para a transição
        #     vazio→populado refletir já no próximo commit.
        try:
            from okto_pulse.core.kg.memory_pressure_collector import (
                record_write_success,
            )

            record_write_success(session.board_id)
        except Exception:  # pragma: no cover — defensivo, nunca quebra commit
            pass
        _COMMIT_HEALTH_CACHE.pop(session.board_id, None)

        return CommitConsolidationResponse(
            session_id=req.session_id,
            status=SessionStatus.COMMITTED,
            nodes_added=counters.nodes_added,
            nodes_updated=counters.nodes_updated,
            nodes_superseded=counters.nodes_superseded,
            edges_added=counters.edges_added,
            nodes_merged=counters.nodes_merged,
            nodes_noop=counters.nodes_noop,
            processed_candidates=(
                counters.nodes_added
                + counters.nodes_updated
                + counters.nodes_merged
                + counters.nodes_superseded
                + counters.nodes_noop
            ),
            merge_audit_items=list(counters.merge_audit_items),
            connectivity=connectivity,
            committed_at=committed_at,
        )


def _resolve_op(
    hint: ReconciliationHint | None, default_confidence: float
) -> ReconciliationOperation:
    if hint is None:
        return ReconciliationOperation.ADD
    op = hint.operation
    if isinstance(op, ReconciliationOperation):
        return op
    return ReconciliationOperation(op)


def _enum_value(obj):
    return obj.value if hasattr(obj, "value") else obj


def _lookup_existing_node(
    kconn, node_type: str, source_artifact_ref: str
) -> str | None:
    """Lookup an existing Kùzu node by type and source_artifact_ref.

    Returns the kuzu_node_id if found, None otherwise. Used when NOOP
    to find existing nodes so edges can still be resolved.
    """
    if not source_artifact_ref:
        return None
    cypher = (
        f"MATCH (n:{node_type}) "
        f"WHERE n.source_artifact_ref = $ref "
        f"RETURN n.id LIMIT 1"
    )
    try:
        res = kconn.execute(cypher, {"ref": source_artifact_ref})
        try:
            if res.has_next():
                return res.get_next()[0]
        finally:
            try:
                res.close()
            except Exception:
                pass
    except Exception:
        pass
    return None


def _node_is_human_curated(kconn, node_type: str, node_id: str) -> bool:
    """Check whether a Kùzu node has the human_curated flag set.

    Treats NULL as FALSE — legacy nodes from before v0.3.2 have no value
    set and must default to agent-managed semantics for retrocompat.
    Returns False on any read error so the UPDATE path defaults to the
    legacy behaviour rather than silently swallowing edits.
    """
    if not node_id:
        return False
    cypher = (
        f"MATCH (n:{node_type}) "
        f"WHERE n.id = $id "
        f"RETURN n.human_curated LIMIT 1"
    )
    try:
        res = kconn.execute(cypher, {"id": node_id})
        try:
            if res.has_next():
                value = res.get_next()[0]
                return bool(value) if value is not None else False
        finally:
            try:
                res.close()
            except Exception:
                pass
    except Exception:
        pass
    return False


_CROSS_SESSION_PREFIXES: tuple[str, ...] = (
    "story_",
    "ideation_",
    "refinement_",
    "spec_",
    "sprint_",
    "card_",
)


def _is_cross_session_entity_ref(endpoint: str) -> bool:
    """Match the `<artifact_type>_<short>_entity` shape Layer 1 emits when
    referencing a parent Entity committed by an earlier session.

    The validation in `add_edge_candidate` accepts these as deferred
    endpoints; `_resolve_endpoint` later does the actual Kùzu lookup at
    commit time.
    """
    if not endpoint.endswith("_entity"):
        return False
    body = endpoint[: -len("_entity")]
    return any(body.startswith(p) and len(body) > len(p) for p in _CROSS_SESSION_PREFIXES)


def _resolve_endpoint(
    endpoint: str,
    candidate_to_kuzu_id: dict[str, str],
    *,
    kconn=None,
) -> tuple[str | None, str | None]:
    """Resolve an edge endpoint to an existing or newly-created kuzu_node_id.

    Returns ``(node_id, node_type)``. ``node_type`` is non-None only when the
    resolution required a Kùzu lookup (cross-session ref) — single-session
    candidates are typed by the caller via the local NodeCandidate.

    Resolution order:
        1. ``kg:<id>`` literal — strip prefix and trust the caller.
        2. Local session candidate — match by candidate_id in the supplied map.
        3. Cross-session by deterministic id pattern (`spec_<short>_entity` /
           `sprint_<short>_entity`) — derive ``source_artifact_ref`` and
           probe Kùzu for an Entity with that ref. Used by Layer 1 to wire
           Sprint→Spec / Card→Sprint hierarchy edges across sessions.
    """
    if endpoint.startswith("kg:"):
        node_id = endpoint[3:]
        if kconn is not None:
            return node_id, _lookup_node_type_by_id(kconn, node_id)
        return node_id, None
    local = candidate_to_kuzu_id.get(endpoint)
    if local is not None:
        return local, None
    if kconn is None:
        return None, None
    # Cross-session deterministic-id fallback. We only handle the worker's
    # own naming convention here (`<artifact>_<id8>_entity`) to avoid
    # surprises; new patterns must be opt-in.
    if endpoint.endswith("_entity"):
        body = endpoint[:-len("_entity")]
        for prefix, ref_prefix in (
            ("story_", "story:"),
            ("ideation_", "ideation:"),
            ("refinement_", "refinement:"),
            ("spec_", "spec:"),
            ("sprint_", "sprint:"),
            ("card_", "card:"),
        ):
            if body.startswith(prefix):
                short = body[len(prefix):]
                # Source_artifact_ref uses the full UUID. We probe with a
                # prefix match because the worker only carries the first 8
                # chars in the candidate id.
                cypher = (
                    "MATCH (n:Entity) "
                    "WHERE n.source_artifact_ref STARTS WITH $ref "
                    "RETURN n.id LIMIT 1"
                )
                try:
                    res = kconn.execute(cypher, {"ref": f"{ref_prefix}{short}"})
                    try:
                        if res.has_next():
                            return res.get_next()[0], "Entity"
                    finally:
                        try:
                            res.close()
                        except Exception:
                            pass
                except Exception:
                    pass
                break
    return None, None


_NODE_UPDATEABLE_ATTRS: frozenset[str] = frozenset({
    "title", "content", "context", "justification",
    "priority_boost", "source_confidence",
    # Maturity METADATA (card 302044a7 / FR4 / dec_85ba8dc2): graph_layer +
    # maturity_status are safe to PROMOTE on a merge by source_artifact_ref
    # (e.g. working->canonical when the source spec reaches done). They are
    # maturity metadata, NOT curated content — so they update even for
    # human_curated nodes, while title/content/context/justification stay
    # protected. Historical/HNSW-locked fields (embedding, created_at,
    # query_hits, human_curated, …) remain excluded.
    "graph_layer", "maturity_status",
})

# Kuzu HNSW vector indexes (see `VECTOR_INDEX_TYPES` in schema.py) own
# the `embedding` column on Decision/Criterion/Constraint/Entity/Learning
# tables and reject direct `SET n.embedding = ...` writes with
# "Cannot set property vec in table embeddings because it is used in one
# or more indexes." A safe rewrite would require DROP INDEX → UPDATE →
# CREATE INDEX which rebuilds the entire HNSW for the table — too costly
# for a per-commit dedup branch. Embeddings therefore stay frozen at
# creation time on the dedup-reuse path. Acceptable trade-off: title /
# content drift is small per re-consolidation; a follow-up worker can
# rebuild stale embeddings in batch when the gap exceeds a threshold.


def _apply_kuzu_node_update_partial(
    orch, node_type: str, node_id: str, attrs: dict
) -> None:
    """Spec 7f23535f (NC-8): UPDATE attrs on existing Kuzu node by id.

    Used by `_do_kuzu_commit` when source_artifact_ref already maps to a
    node — preserves historical fields (created_at, created_by_agent,
    query_hits, last_queried_at, relevance_score, source_session_id,
    human_curated) and refreshes only content-derived attrs.

    Filters input via `_NODE_UPDATEABLE_ATTRS` to ensure no historical
    field can be accidentally clobbered if the caller passes extras —
    notably `embedding` (HNSW-locked) and `created_at` / `query_hits`
    (historical).
    """
    set_pairs: list[str] = []
    params: dict = {"node_id": node_id}
    for k, v in attrs.items():
        if k in _NODE_UPDATEABLE_ATTRS:
            set_pairs.append(f"n.{k} = ${k}")
            params[k] = v
    if not set_pairs:
        return
    cypher = (
        f"MATCH (n:{node_type}) "
        f"WHERE n.id = $node_id "
        f"SET {', '.join(set_pairs)}"
    )
    orch.kuzu_conn.execute(cypher, params)
    # Note: do NOT append a KuzuWriteRecord — the existing node was created
    # by a prior session, and compensation rollback uses source_session_id
    # to scope deletes. Re-recording here would risk deleting the node on
    # rollback of the current session despite belonging to the prior one.


def _apply_kuzu_node_create_with_timestamp(
    orch, node_type: str, node_id: str, attrs: dict
) -> None:
    """Shim that uses the orchestrator's create_node but handles the Kùzu
    timestamp() wrapper around created_at. Kùzu's parameter binding can't
    coerce an ISO string to TIMESTAMP without the timestamp() function call,
    so we patch the generated query before execution.
    """
    # orchestrator.create_node builds a literal `created_at: $created_at`
    # substring — we rewrite the connection.execute call to wrap it.
    # Simpler approach: pre-create the node with raw kuzu_conn and then append
    # to records manually so compensation still works.
    params = dict(attrs)
    params["id"] = node_id
    params["source_session_id"] = orch.session_id
    columns = ", ".join(
        f"{k}: timestamp(${k})" if k == "created_at" else f"{k}: ${k}"
        for k in params
    )
    orch.kuzu_conn.execute(
        f"CREATE (n:{node_type} {{{columns}}})", params
    )
    from okto_pulse.core.kg.transaction import KuzuWriteRecord
    orch.records.append(
        KuzuWriteRecord(kind="node", entity_type=node_type, entity_id=node_id)
    )
    orch.counters.nodes_added += 1


# ---------------------------------------------------------------------------
# 7. abort_consolidation
# ---------------------------------------------------------------------------


async def abort_consolidation(
    req: AbortConsolidationRequest,
    *,
    agent_id: str,
) -> AbortConsolidationResponse:
    """Drop an in-flight session. No compensating delete because commit was
    never called — the transactional boundary guaranteed no partial writes."""
    session = await _require_open_session(req.session_id, agent_id)
    async with session.lock:
        session.status = SessionStatus.ABORTED
    await get_kg_registry().require_session_store().remove(req.session_id)
    return AbortConsolidationResponse(
        session_id=req.session_id,
        status=SessionStatus.ABORTED,
        compensating_delete_applied=False,
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")


# ---------------------------------------------------------------------------
# Audit helpers — registry.audit_repo only
# ---------------------------------------------------------------------------


async def _get_latest_audit(registry, db, board_id: str, artifact_id: str):
    """Get latest committed audit via the composed AuditRepository."""
    if registry.audit_repo is not None:
        return await registry.audit_repo.get_latest_for_artifact(board_id, artifact_id)
    raise KGPrimitiveError(
        "audit_repository_required",
        (
            "KG consolidation requires a composed audit_repo. The core no longer "
            "falls back to direct DB audit/outbox access (R-P2-02); configure the "
            "registry with an AuditRepository adapter."
        ),
    )


def _audit_hash(audit_row) -> str | None:
    """Extract content_hash from either AuditRow DTO or SQLAlchemy model."""
    return audit_row.content_hash if audit_row else None


def _audit_session_id(audit_row) -> str | None:
    """Extract session_id from either AuditRow DTO or SQLAlchemy model."""
    return audit_row.session_id if audit_row else None


async def _commit_audit_records(registry, db, records, counters, req, session, agent_id, committed_at):
    """Write audit records via the composed AuditRepository.

    Args:
        records: List of orch record objects (entity_id, entity_type, kind).
        counters: CommitCounters with nodes_added/updated/superseded, edges_added.
    """
    from okto_pulse.core.kg.interfaces.audit_dtos import (
        ConsolidationAuditData,
        NodeRefData,
        OutboxEventData,
    )

    kuzu_refs = [
        NodeRefData(
            session_id=req.session_id,
            board_id=session.board_id,
            kuzu_node_id=r.entity_id,
            kuzu_node_type=r.entity_type,
            operation="add" if r.kind == "node" else "edge",
        )
        for r in records
        if r.kind == "node"
    ]

    audit_data = ConsolidationAuditData(
        session_id=req.session_id,
        board_id=session.board_id,
        artifact_id=session.artifact_id,
        artifact_type=session.artifact_type,
        agent_id=agent_id,
        started_at=session.started_at,
        committed_at=committed_at,
        nodes_added=counters.nodes_added,
        nodes_updated=counters.nodes_updated,
        nodes_superseded=counters.nodes_superseded,
        edges_added=counters.edges_added,
        summary_text=req.summary_text,
        content_hash=session.content_hash,
    )

    outbox_data = OutboxEventData(
        event_id=f"evt_{uuid.uuid4().hex[:16]}",
        board_id=session.board_id,
        session_id=req.session_id,
        event_type="consolidation_committed",
        payload={
            "session_id": req.session_id,
            "artifact_id": session.artifact_id,
            "nodes_added": counters.nodes_added,
            "nodes_updated": counters.nodes_updated,
            "nodes_superseded": counters.nodes_superseded,
            "edges_added": counters.edges_added,
        },
    )

    if registry.audit_repo is not None:
        await registry.audit_repo.commit_consolidation_records(
            audit_data, kuzu_refs, outbox_data
        )
        return
    raise KGPrimitiveError(
        "audit_repository_required",
        (
            "KG consolidation requires a composed audit_repo. The core no longer "
            "writes audit/outbox rows via direct DB fallback (R-P2-02); configure "
            "the registry with an AuditRepository adapter."
        ),
        session_id=req.session_id,
    )
