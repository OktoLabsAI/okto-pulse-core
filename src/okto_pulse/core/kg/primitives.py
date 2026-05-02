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

from okto_pulse.core.kg.interfaces.registry import get_kg_registry
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
    GetSimilarNodesRequest,
    GetSimilarNodesResponse,
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
    from okto_pulse.core.kg.schema import MULTI_REL_TYPES, REL_TYPES

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
    store = get_kg_registry().session_store
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
) -> BeginConsolidationResponse:
    """Open a new transactional session. SHA256-dedup against the last commit."""
    registry = get_kg_registry()
    store = registry.session_store
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"

    content_hash = compute_content_hash(req.raw_content, req.artifact_id, req.board_id)

    # Nothing-changed detection via audit repository or db fallback.
    # When both db=None and no audit_repo, skip dedup entirely — forced
    # re-processing so stale audit records with matching content_hash
    # don't cause all candidates to be marked NOOP (0 nodes written).
    has_audit_source = db is not None or registry.audit_repo is not None
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
    session = await _require_open_session(req.session_id, agent_id)
    store = get_kg_registry().session_store
    async with session.lock:
        if req.candidate.candidate_id in session.node_candidates:
            raise KGPrimitiveError(
                "duplicate_candidate_id",
                f"candidate_id already in session: {req.candidate.candidate_id}",
                session_id=req.session_id,
            )
        session.node_candidates[req.candidate.candidate_id] = req.candidate
        session.touch(store.default_ttl_seconds)
        return AddNodeCandidateResponse(
            session_id=req.session_id,
            candidate_id=req.candidate.candidate_id,
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
    store = get_kg_registry().session_store
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
            raise KGPrimitiveError(
                "layer_violation",
                str(LayerViolationError(edge_type_str)),
                session_id=req.session_id,
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

    Embeds the candidate with the active embedding provider (stub or
    sentence-transformers) and runs a k-NN query against the per-type HNSW
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
    embedder = get_kg_registry().embedding_provider
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
    """Sync: find existing Kùzu nodes matching session candidates.

    Runs in the thread pool via ``_run_kuzu``.
    """
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.search import find_similar_for_candidate

    existing_matches: dict[str, list] = {}
    conn = open_board_connection(board_id)
    try:
        with conn as (_db, kconn):
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
                    conn=kconn,
                )
                if matches:
                    existing_matches[cand_id] = matches
    except Exception as exc:
        logger.warning(
            "kg.primitives.reconciliation_search_failed err=%s", exc,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return existing_matches


async def propose_reconciliation(
    req: ProposeReconciliationRequest,
    *,
    agent_id: str,
    db=None,
) -> ProposeReconciliationResponse:
    """Compute deterministic ADD/UPDATE/SUPERSEDE/NOOP hints for all candidates."""
    from okto_pulse.core.kg.reconciliation import reconcile_session

    registry = get_kg_registry()
    session = await _require_open_session(req.session_id, agent_id)

    if db is not None or registry.audit_repo is not None:
        latest = await _get_latest_audit(
            registry, db, session.board_id, session.artifact_id
        )
        nothing_changed = bool(latest and _audit_hash(latest) == session.content_hash)
    else:
        nothing_changed = False

    existing_matches_by_candidate: dict[str, list] = {}
    if not nothing_changed:
        embedder = registry.embedding_provider
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
        session.touch(registry.session_store.default_ttl_seconds)

    return ProposeReconciliationResponse(session_id=req.session_id, hints=hints)


# ---------------------------------------------------------------------------
# 6. commit_consolidation
# ---------------------------------------------------------------------------


def _compensate_kuzu_writes(board_id: str, session_id: str, records: list) -> None:
    """Sync: reverse Kùzu writes for a failed commit.

    Mirrors ``TransactionOrchestrator.compensate()`` but runs synchronously
    inside the thread pool. Best-effort — logs failures but does not raise.
    """
    from okto_pulse.core.kg.schema import (
        MULTI_REL_TYPES,
        REL_TYPES,
        open_board_connection,
    )

    try:
        with open_board_connection(board_id) as (_db, kconn):
            # Delete edges first (they reference nodes)
            rel_pairs = list(REL_TYPES)
            for rel_name, endpoint_pairs in MULTI_REL_TYPES:
                rel_pairs.extend(
                    (rel_name, from_type, to_type)
                    for from_type, to_type in endpoint_pairs
                )
            for rel_name, from_type, to_type in rel_pairs:
                try:
                    kconn.execute(
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
                    kconn.execute(
                        f"MATCH (n:{node_type}) "
                        f"WHERE n.source_session_id = $sid DETACH DELETE n",
                        {"sid": session_id},
                    )
                except Exception:
                    pass
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


def _do_kuzu_commit(
    board_id: str,
    session_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    effective_hints: dict,
    agent_id: str,
    embedder,
) -> tuple[dict, object, list, datetime]:
    """Synchronous Kùzu writes for ``commit_consolidation``.

    Runs in the thread pool via ``_run_kuzu``. Returns
    ``(candidate_to_kuzu_id, counters, records, committed_at)`` on success.
    Raises ``KGPrimitiveError`` on failure (after inline compensation).
    """
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    board_conn = open_board_connection(board_id)
    with board_conn as (_kdb, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,  # SQLite writes happen in async context
            session_id=session_id,
            board_id=board_id,
        )
        candidate_to_kuzu_id: dict[str, str] = {}
        candidate_to_node_type: dict[str, str] = {}

        try:
            for cand_id, cand in node_candidates.items():
                hint = effective_hints.get(cand_id)
                op = _resolve_op(hint, cand.source_confidence)
                node_type = _enum_value(cand.node_type)

                if op == ReconciliationOperation.NOOP:
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
                                "source_confidence": cand.source_confidence,
                                "priority_boost": getattr(cand, "priority_boost", 0.0),
                                "embedding": embedding,
                            }
                            _apply_kuzu_node_update_partial(
                                orch, node_type, existing_id, update_attrs
                            )
                        candidate_to_kuzu_id[cand_id] = existing_id
                        candidate_to_node_type[cand_id] = node_type
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
            return (
                candidate_to_kuzu_id,
                orch.counters,
                list(orch.records),
                committed_at,
            )

        except KGPrimitiveError:
            raise
        except Exception as exc:
            _compensate_kuzu_writes(board_id, session_id, orch.records)
            raise KGPrimitiveError(
                "commit_failed",
                f"commit failed and was compensated: {exc}",
                session_id=session_id,
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
    """
    registry = get_kg_registry()
    session = await _require_open_session(req.session_id, agent_id)

    async with session.lock:
        effective_hints = dict(session.reconciliation_hints)
        for cid, override in req.agent_overrides.items():
            effective_hints[cid] = override

        # --- Kùzu writes (offloaded to thread pool) ---
        try:
            candidate_to_kuzu_id, counters, records, committed_at = await _run_kuzu(
                _do_kuzu_commit,
                session.board_id,
                req.session_id,
                dict(session.node_candidates),
                dict(session.edge_candidates),
                effective_hints,
                agent_id,
                registry.embedding_provider,
            )
        except KGPrimitiveError:
            raise
        except Exception as exc:
            raise KGPrimitiveError(
                "commit_failed",
                f"Kùzu commit failed: {exc}",
                session_id=req.session_id,
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
        await registry.session_store.remove(req.session_id)

        registry.cache_backend.invalidate_board(session.board_id)

        return CommitConsolidationResponse(
            session_id=req.session_id,
            status=SessionStatus.COMMITTED,
            nodes_added=counters.nodes_added,
            nodes_updated=counters.nodes_updated,
            nodes_superseded=counters.nodes_superseded,
            edges_added=counters.edges_added,
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


_CROSS_SESSION_PREFIXES: tuple[str, ...] = ("spec_", "sprint_", "card_")


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
        return endpoint[3:], None
    local = candidate_to_kuzu_id.get(endpoint)
    if local is not None:
        return local, None
    if kconn is None:
        return None, None
    # Cross-session deterministic-id fallback. We only handle the worker's
    # own naming convention here (`spec_<id8>_entity`, `sprint_<id8>_entity`)
    # to avoid surprises; new patterns must be opt-in.
    if endpoint.endswith("_entity"):
        body = endpoint[:-len("_entity")]
        for prefix, ref_prefix in (("spec_", "spec:"), ("sprint_", "sprint:"), ("card_", "card:")):
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
    await get_kg_registry().session_store.remove(req.session_id)
    return AbortConsolidationResponse(
        session_id=req.session_id,
        status=SessionStatus.ABORTED,
        compensating_delete_applied=False,
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")


# ---------------------------------------------------------------------------
# Audit helpers — registry.audit_repo with db fallback
# ---------------------------------------------------------------------------


async def _get_latest_audit(registry, db, board_id: str, artifact_id: str):
    """Get latest committed audit via audit_repo or db fallback."""
    if registry.audit_repo is not None:
        return await registry.audit_repo.get_latest_for_artifact(board_id, artifact_id)
    if db is not None:
        from sqlalchemy import select
        from okto_pulse.core.models.db import ConsolidationAudit

        query = (
            select(ConsolidationAudit)
            .where(
                ConsolidationAudit.board_id == board_id,
                ConsolidationAudit.artifact_id == artifact_id,
                ConsolidationAudit.committed_at.is_not(None),
                ConsolidationAudit.undo_status == "none",
            )
            .order_by(ConsolidationAudit.committed_at.desc())
            .limit(1)
        )
        return (await db.execute(query)).scalars().first()
    return None


def _audit_hash(audit_row) -> str | None:
    """Extract content_hash from either AuditRow DTO or SQLAlchemy model."""
    return audit_row.content_hash if audit_row else None


def _audit_session_id(audit_row) -> str | None:
    """Extract session_id from either AuditRow DTO or SQLAlchemy model."""
    return audit_row.session_id if audit_row else None


async def _commit_audit_records(registry, db, records, counters, req, session, agent_id, committed_at):
    """Write audit records via audit_repo or db fallback.

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
    elif db is not None:
        from okto_pulse.core.models.db import (
            ConsolidationAudit,
            GlobalUpdateOutbox,
            KuzuNodeRef,
        )

        db.add(ConsolidationAudit(
            session_id=audit_data.session_id,
            board_id=audit_data.board_id,
            artifact_id=audit_data.artifact_id,
            artifact_type=audit_data.artifact_type,
            agent_id=audit_data.agent_id,
            started_at=audit_data.started_at,
            committed_at=audit_data.committed_at,
            nodes_added=audit_data.nodes_added,
            nodes_updated=audit_data.nodes_updated,
            nodes_superseded=audit_data.nodes_superseded,
            edges_added=audit_data.edges_added,
            summary_text=audit_data.summary_text,
            content_hash=audit_data.content_hash,
            undo_status="none",
        ))
        for ref in kuzu_refs:
            db.add(KuzuNodeRef(
                session_id=ref.session_id,
                board_id=ref.board_id,
                kuzu_node_id=ref.kuzu_node_id,
                kuzu_node_type=ref.kuzu_node_type,
                operation=ref.operation,
            ))
        db.add(GlobalUpdateOutbox(
            event_id=outbox_data.event_id,
            board_id=outbox_data.board_id,
            session_id=outbox_data.session_id,
            event_type=outbox_data.event_type,
            payload=outbox_data.payload,
        ))
        await db.commit()
