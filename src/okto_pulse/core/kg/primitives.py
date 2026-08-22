"""The 7 consolidation primitives — pure async functions (no MCP decoration).

Each primitive takes a typed Pydantic request and returns a typed response.
The MCP layer in `okto_pulse.core.mcp.kg_tools` wraps these and handles
auth/serialization. Keeping the primitives decoupled from MCP means the REST
API (spec 3681b078) can reuse the same functions.

Reconciliation rules (deterministic, zero LLM):
- SHA256 content_hash matches last committed → NOOP
- Candidate has stable id that matches existing graph node → UPDATE
- Candidate similar to existing node (embedding + title fuzzy match) but
  new id → SUPERSEDE hint, agent decides whether to override
- Otherwise → ADD

commit_consolidation writes to the graph backend first, releases the embedded
graph writer, appends the durable cognitive-source batch, then stages the audit
row + outbox event.  This is an explicit saga: embedded graph statements may
auto-commit, so there is a bounded graph-ahead window between graph close and
the durable append.  Append failure triggers best-effort graph compensation
before the stable fail-closed error is returned.  Compensation removes
session-created nodes/edges; in-place UPDATE/NC-8 mutations have no before-image
in the current orchestrator and therefore require retry/reconciliation when an
immutable same-generation source conflict exposes that saga residue.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import TypeVar

from okto_pulse.core.domain.code_traceability_kg import (
    CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH,
    CodeTraceabilityKGWriteViolation,
    is_code_traceability_subtype,
    require_code_traceability_candidate_writer,
)
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.runtime_context import runtime_state
from okto_pulse.core.kg.node_identity import (
    derive_natural_key,
    mint_node_id,
    normalize_text,
)
from okto_pulse.core.kg.relational_projection import (
    parse_relational_projection_ref,
    relational_projection_alternative_relation_rule,
    relational_projection_belongs_to_rule,
    relational_projection_candidate_id,
    relational_projection_edge_id,
)
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
from okto_pulse.core.kg.guarded_write import (
    GuardedWriteError,
    guarded_board_write,
)
from okto_pulse.core.kg.interfaces.graph_transaction import (
    SpecLineageParentIntent,
    SpecLineageReconciliationError,
    is_spec_lineage_rule_id,
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
    NodeCandidate,
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
from okto_pulse.core.ports.runtime_workers import BlockingExecutionPort

logger = logging.getLogger("okto_pulse.kg.primitives")

_T = TypeVar("_T")


@dataclass
class _PendingConsolidationCommit:
    """Graph-applied commit awaiting a caller-owned relational commit.

    Sessions are process-local, so this snapshot intentionally stays an
    internal in-memory recovery record.  It contains exactly what is needed to
    restage the durable cognitive ledger plus audit/outbox on retry, while
    skipping ``_do_graph_commit`` entirely.
    """

    request_payload: dict[str, object]
    records: tuple[object, ...]
    counters: object
    cognitive_source_records: tuple[dict, ...]
    response: CommitConsolidationResponse
    in_flight: bool = False


def _allowed_edge_pairs(edge_type: str) -> tuple[tuple[str, str], ...]:
    from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, REL_TYPES

    pairs = [
        (from_type, to_type)
        for rel, from_type, to_type in REL_TYPES
        if rel == edge_type
    ]
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
                {"from_type": src, "to_type": dst} for src, dst in allowed
            ],
        },
    )


async def _run_graph_io(
    func,
    *args,
    executor: BlockingExecutionPort | None = None,
    **kwargs,
):
    """Run synchronous graph IO without losing its lifetime on cancellation.

    Edition workers pass their tracked executor so shutdown can join an
    in-flight native graph call. Direct callers retain a cancellation-drained
    fallback: the parent does not disappear while an untracked thread still
    owns the process writer lease.
    """

    operation = partial(func, *args, **kwargs)
    if executor is not None:
        return await executor.run(operation)
    task = asyncio.create_task(
        asyncio.to_thread(operation),
        name="core.kg.graph_io",
    )
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.gather(task, return_exceptions=True)
        raise


async def _run_cancellation_atomic(
    operation: Awaitable[_T],
    *,
    task_name: str,
) -> _T:
    """Finish a commit critical section before propagating cancellation.

    ``asyncio.shield`` prevents the parent cancellation from reaching the
    critical task.  The drain loop also tolerates repeated cancellation of
    the parent, so graph commit, durable audit/outbox persistence, and session
    finalization cannot be split across separate task lifetimes.
    """

    task = asyncio.create_task(operation, name=task_name)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break

        if task.done() and not task.cancelled():
            try:
                task.result()
            except BaseException as exc:
                logger.error(
                    "kg.commit.cancel_drain_failed task=%s error_type=%s",
                    task_name,
                    type(exc).__name__,
                )
        raise


async def run_cancellation_atomic(
    operation: Awaitable[_T],
    *,
    task_name: str,
) -> _T:
    """Public Core helper for a short cross-store completion boundary.

    Adapters use this when a relational commit and its process-local
    finalizer must finish as one cancellation-drained operation.
    """

    return await _run_cancellation_atomic(operation, task_name=task_name)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class KGPrimitiveError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        session_id: str | None = None,
        details: dict | None = None,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.session_id = session_id
        self.details = details or {}
        self.retryable = bool(retryable)


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


def _contextualize_graph_commit_error(exc: BaseException) -> tuple[str, dict]:
    """Preserve semantic adapter error context without interpreting a backend."""

    details = getattr(exc, "details", None)
    context = dict(details) if isinstance(details, dict) else {}
    if isinstance(exc, SpecLineageReconciliationError):
        receipt = exc.receipt
        cause = exc.__cause__
        context.update(
            {
                "failure_type": type(exc).__name__,
                "spec_lineage_error_code": exc.code,
                "retryable": exc.retryable,
                "compensation_applied": exc.compensation_applied,
                "progress_preserved": exc.preserve_progress,
                "cause_type": type(cause).__name__ if cause is not None else None,
                "cause_code": getattr(cause, "code", None),
            }
        )
        if receipt is not None:
            context.update(
                {
                    "source_id": receipt.source_id,
                    "target_id": receipt.target_id,
                    "target_rule_id": receipt.target_rule_id,
                    "new_edge_created": receipt.new_edge_created,
                    "removed_edge_count": len(receipt.removed_edges),
                    "ambiguous_legacy_edges": receipt.ambiguous_legacy_edges,
                }
            )
    return str(exc), context


def _graph_commit_failure_code(exc: BaseException) -> tuple[str, bool]:
    """Project a stable semantic code without flattening typed graph errors."""

    if isinstance(exc, SpecLineageReconciliationError):
        return exc.code, exc.retryable
    return "commit_failed", False


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


def _validate_session_state(
    session: ConsolidationSession,
    *,
    allow_pending_commit: bool,
) -> None:
    """Validate mutable session state; callers under ``session.lock`` re-use it.

    The first lookup is necessarily optimistic.  A commit can create a
    deferred snapshot while another coroutine waits for the same lock, so
    every mutating boundary must repeat this check after acquiring the lock.
    """

    session_id = session.session_id
    if session.status != SessionStatus.OPEN:
        raise KGPrimitiveError(
            "session_already_committed",
            f"Session {session_id} is in status {session.status}",
            session_id=session_id,
        )
    pending = getattr(session, "pending_commit", None)
    if pending is not None:
        if bool(getattr(pending, "in_flight", False)):
            raise KGPrimitiveError(
                "session_commit_in_progress",
                f"Session {session_id} is awaiting relational commit finalization",
                session_id=session_id,
            )
        if not allow_pending_commit:
            raise KGPrimitiveError(
                "session_commit_pending",
                f"Session {session_id} has a graph-applied commit awaiting retry",
                session_id=session_id,
            )


async def _require_open_session(
    session_id: str,
    agent_id: str,
    *,
    allow_pending_commit: bool = False,
) -> ConsolidationSession:
    store = get_kg_registry().require_session_store()
    session = await store.get(session_id)
    if session is None:
        raise _not_found(session_id)
    if not session.check_ownership(agent_id):
        raise _ownership(session_id, agent_id)
    _validate_session_state(
        session,
        allow_pending_commit=allow_pending_commit,
    )
    return session


# ---------------------------------------------------------------------------
# 1. begin_consolidation
# ---------------------------------------------------------------------------


_SPEC_LINEAGE_WORKER_AGENT_ID = "system:historical_consolidation"


def _validate_spec_lineage_parent_intent(
    *,
    intent: SpecLineageParentIntent,
    artifact_type: str,
    artifact_id: str,
    agent_id: str,
    node_candidates: dict[str, NodeCandidate],
    edge_candidates: dict[str, EdgeCandidate] | None = None,
    session_id: str | None = None,
    force_reprocess: bool = True,
) -> str | None:
    """Validate the internal clear signal and return its Spec source candidate."""

    if intent is SpecLineageParentIntent.PRESERVE:
        return None
    if (
        artifact_type != "spec"
        or agent_id != _SPEC_LINEAGE_WORKER_AGENT_ID
        or not force_reprocess
    ):
        raise KGPrimitiveError(
            "spec_lineage_clear_intent_forbidden",
            "Spec-lineage clear is an internal forced deterministic-worker operation.",
            session_id=session_id,
            details={
                "artifact_type": artifact_type,
                "agent_id": agent_id,
                "force_reprocess": force_reprocess,
            },
        )

    expected_ref = f"spec:{artifact_id}"
    source_candidates = [
        candidate_id
        for candidate_id, candidate in node_candidates.items()
        if _enum_value(candidate.node_type) == "Entity"
        and str(candidate.source_artifact_ref or "") == expected_ref
    ]
    if len(source_candidates) != 1:
        raise KGPrimitiveError(
            "spec_lineage_clear_source_invalid",
            "Spec-lineage clear requires exactly one canonical Spec Entity "
            "candidate with the session artifact source ref.",
            session_id=session_id,
            details={
                "expected_source_artifact_ref": expected_ref,
                "matching_candidates": sorted(source_candidates),
            },
        )

    conflicting_edges = sorted(
        candidate_id
        for candidate_id, candidate in (edge_candidates or {}).items()
        if is_spec_lineage_rule_id(str(candidate.rule_id or ""))
    )
    if conflicting_edges:
        raise KGPrimitiveError(
            "spec_lineage_clear_conflicts_with_parent",
            "A Spec-lineage clear intent cannot share a session with a "
            "deterministic Spec-parent edge.",
            session_id=session_id,
            details={"conflicting_edge_candidates": conflicting_edges},
        )
    return source_candidates[0]


async def begin_consolidation(
    req: BeginConsolidationRequest,
    *,
    agent_id: str,
    db=None,
    force_reprocess: bool = False,
    spec_lineage_parent_intent: SpecLineageParentIntent = (
        SpecLineageParentIntent.PRESERVE
    ),
    relational_projection_candidate_ids: frozenset[str] = frozenset(),
    relational_projection_active_set_intent: object | None = None,
) -> BeginConsolidationResponse:
    """Open a new transactional session. SHA256-dedup against the last commit."""
    registry = get_kg_registry()
    store = registry.require_session_store()
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"

    deterministic_candidates: dict[str, NodeCandidate] = {}
    for candidate in req.deterministic_candidates:
        if candidate.candidate_id in deterministic_candidates:
            raise KGPrimitiveError(
                "duplicate_candidate_id",
                f"Duplicate deterministic candidate: {candidate.candidate_id}",
                session_id=session_id,
            )
        deterministic_candidates[candidate.candidate_id] = candidate

    _require_code_traceability_candidate_ownership(
        deterministic_candidates,
        agent_id=agent_id,
        session_id=session_id,
    )

    try:
        lineage_intent = SpecLineageParentIntent(spec_lineage_parent_intent)
    except ValueError as exc:
        raise KGPrimitiveError(
            "spec_lineage_parent_intent_invalid",
            "Unknown Spec-lineage parent intent.",
            session_id=session_id,
        ) from exc
    _validate_spec_lineage_parent_intent(
        intent=lineage_intent,
        artifact_type=req.artifact_type,
        artifact_id=req.artifact_id,
        agent_id=agent_id,
        node_candidates=deterministic_candidates,
        session_id=session_id,
        force_reprocess=force_reprocess,
    )

    projection_candidate_ids = frozenset(
        str(candidate_id) for candidate_id in relational_projection_candidate_ids
    )
    projection_intent = relational_projection_active_set_intent
    if projection_intent is not None:
        owner_type = str(getattr(projection_intent, "owner_type", ""))
        owner_id = str(getattr(projection_intent, "owner_id", ""))
        namespace = str(getattr(projection_intent, "namespace", ""))
        active_refs = tuple(getattr(projection_intent, "active_refs", ()))
        active_edges = tuple(getattr(projection_intent, "active_edges", ()))
        supported_scope = (req.artifact_type, owner_type, namespace) in {
            ("refinement", "refinement", "rdl"),
            ("spec", "spec", "dependencies"),
        }
        if (
            not agent_id.startswith("system:")
            or not supported_scope
            or owner_id != req.artifact_id
        ):
            raise KGPrimitiveError(
                "relational_projection_scope_invalid",
                "Relational projection ownership is restricted to the "
                "server-side refinement/RDL and Spec dependency paths.",
                session_id=session_id,
            )
        active_candidate_ids = frozenset(
            str(getattr(ref, "candidate_id", "")) for ref in active_refs
        )
        if (
            "" in active_candidate_ids
            or active_candidate_ids != projection_candidate_ids
        ):
            raise KGPrimitiveError(
                "relational_projection_active_set_mismatch",
                "Projection candidate ids must exactly match the active-set "
                "member identities.",
                session_id=session_id,
            )
        for ref in active_refs:
            candidate_id = str(getattr(ref, "candidate_id", ""))
            candidate = deterministic_candidates.get(candidate_id)
            if candidate is None:
                raise KGPrimitiveError(
                    "relational_projection_candidate_missing",
                    "An active relational projection candidate is absent from "
                    "the deterministic candidate set.",
                    session_id=session_id,
                )
            if _enum_value(candidate.node_type) != str(
                getattr(ref, "node_type", "")
            ) or str(candidate.source_artifact_ref or "") != str(
                getattr(ref, "source_artifact_ref", "")
            ):
                raise KGPrimitiveError(
                    "relational_projection_candidate_identity_mismatch",
                    "An active relational projection member does not match its "
                    "deterministic candidate identity.",
                    session_id=session_id,
                )
        if namespace == "rdl" and active_edges:
            raise KGPrimitiveError(
                "relational_projection_active_set_mismatch",
                "The refinement/RDL projection cannot own operational edges.",
                session_id=session_id,
            )
        if namespace == "dependencies":
            if active_refs or projection_candidate_ids:
                raise KGPrimitiveError(
                    "relational_projection_active_set_mismatch",
                    "The Spec dependency projection owns edges, not nodes.",
                    session_id=session_id,
                )
            owner_candidates = {
                candidate_id
                for candidate_id, candidate in deterministic_candidates.items()
                if _enum_value(candidate.node_type) == "Entity"
                and str(candidate.source_artifact_ref or "") == f"spec:{owner_id}"
            }
            if len(owner_candidates) != 1:
                raise KGPrimitiveError(
                    "relational_projection_owner_unresolved",
                    "The Spec dependency owner root must be present exactly once.",
                    session_id=session_id,
                )
            owner_candidate_id = next(iter(owner_candidates))
            edge_candidate_ids: set[str] = set()
            for edge_ref in active_edges:
                candidate_id = str(getattr(edge_ref, "candidate_id", ""))
                from_candidate_id = str(getattr(edge_ref, "from_candidate_id", ""))
                to_candidate_id = str(getattr(edge_ref, "to_candidate_id", ""))
                rule_id = str(getattr(edge_ref, "rule_id", ""))
                source_ref_endpoint = _parse_source_ref_endpoint(from_candidate_id)
                prerequisite_reference_valid = bool(
                    source_ref_endpoint is not None
                    and source_ref_endpoint[0] == "Entity"
                    and _is_spec_root_source_ref(source_ref_endpoint[1])
                    and source_ref_endpoint[1] != f"spec:{owner_id}"
                )
                if (
                    not candidate_id
                    or candidate_id in edge_candidate_ids
                    or str(getattr(edge_ref, "edge_type", "")) != "precedes"
                    or to_candidate_id != owner_candidate_id
                    or (
                        not prerequisite_reference_valid
                        and (
                            from_candidate_id not in deterministic_candidates
                            or _enum_value(
                                deterministic_candidates[from_candidate_id].node_type
                            )
                            != "Entity"
                        )
                    )
                    or not rule_id.startswith("precedes/spec_dependency/")
                ):
                    raise KGPrimitiveError(
                        "relational_projection_edge_identity_mismatch",
                        "A Spec dependency edge is outside its exact projection scope.",
                        session_id=session_id,
                    )
                edge_candidate_ids.add(candidate_id)
    elif projection_candidate_ids:
        raise KGPrimitiveError(
            "relational_projection_intent_required",
            "Relational projection candidates require an exact active-set intent.",
            session_id=session_id,
        )

    content_hash = compute_content_hash(req.raw_content, req.artifact_id, req.board_id)

    # Nothing-changed detection is owned by the composed AuditRepository.
    # R-P2-02 removed the silent DB fallback: a runtime without audit_repo is
    # mis-composed and must fail early instead of reading relational tables via
    # an escape path.
    has_audit_source = not force_reprocess
    if has_audit_source:
        latest = await _get_latest_audit(
            registry,
            db,
            req.board_id,
            req.artifact_type,
            req.artifact_id,
        )
        nothing_changed = bool(latest and _audit_hash(latest) == content_hash)
        previous_session_id = _audit_session_id(latest) if latest else None
    else:
        nothing_changed = False
        previous_session_id = None

    # Only the unchanged branch can lead to FR5 re-attestation later in this
    # session. Gate it before session creation; fresh/changed begin remains a
    # staging-only operation and commit revalidates health with its real UoW.
    if nothing_changed:
        kg_health_state = await _resolve_commit_kg_health_state(req.board_id, db)
        _validate_degraded_connectivity_before_open(
            board_id=req.board_id,
            session_id=session_id,
            node_candidates=deterministic_candidates,
            edge_candidates={},
            writer_path=_connectivity_writer_path(agent_id),
            kg_health_state=kg_health_state,
        )

    session = await store.create(
        session_id=session_id,
        board_id=req.board_id,
        artifact_id=req.artifact_id,
        artifact_type=req.artifact_type,
        agent_id=agent_id,
        raw_content=req.raw_content,
    )
    session.node_candidates.update(deterministic_candidates)
    session.spec_lineage_parent_intent = lineage_intent
    session.relational_projection_candidate_ids = projection_candidate_ids
    session.relational_projection_active_set_intent = projection_intent

    # Spec 4007e4a3 (Ideação #3, FR6): structured counter for the
    # nothing_changed short-circuit. Lets observability tooling track how
    # often the begin_consolidation idempotency path saves downstream
    # extraction + reconciliation work for unchanged artifacts.
    if nothing_changed:
        logger.info(
            "kg.consolidation.nothing_changed.short_circuit board=%s "
            "artifact_type=%s artifact_id=%s previous_session=%s",
            req.board_id,
            req.artifact_type,
            req.artifact_id,
            previous_session_id,
            extra={
                "event": "kg.consolidation.nothing_changed.short_circuit",
                "board_id": req.board_id,
                "artifact_type": req.artifact_type,
                "artifact_id": req.artifact_id,
                "previous_session_id": previous_session_id,
            },
        )

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

    _require_code_traceability_candidate_ownership(
        {cand.candidate_id: cand},
        agent_id=agent_id,
        session_id=req.session_id,
    )

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
        cand.graph_layer.value
        if hasattr(cand.graph_layer, "value")
        else cand.graph_layer
    )
    try:
        check_cognitive_node_canonical(
            graph_layer_value,
            is_system_worker=is_system_worker,
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
        _validate_session_state(session, allow_pending_commit=False)
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
        _validate_session_state(session, allow_pending_commit=False)
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
            if _parse_source_ref_endpoint(ep) is not None:
                continue
            if ep in session.node_candidates:
                continue
            # Cross-session deterministic refs (Layer 1 hierarchy backbone):
            # `<type>_<short>_entity` points to an Entity committed in a prior
            # session of this board. The actual id resolution is deferred to
            # commit_consolidation via graph backend lookup; here we just accept the
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
    """Return up to top_k existing graph backend nodes similar to the candidate.

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
    _require_code_traceability_candidate_ownership(
        {cand.candidate_id: cand},
        agent_id=agent_id,
        session_id=req.session_id,
    )
    embedder = get_kg_registry().require_embedding_provider()
    query_vec = embedder.encode(f"{cand.title}\n{cand.content or ''}")

    node_type = (
        cand.node_type.value if hasattr(cand.node_type, "value") else cand.node_type
    )
    raw = await _run_graph_io(
        find_similar_nodes_by_type,
        board_id=session.board_id,
        node_type=node_type,
        query_vector=query_vec,
        top_k=req.top_k,
        min_similarity=req.min_similarity,
    )

    similar = [
        SimilarNode(
            graph_node_id=r.graph_node_id,
            node_type=r.node_type,
            title=r.title,
            source_artifact_ref=r.source_artifact_ref,
            similarity=r.similarity,
        )
        for r in raw
        if not is_code_traceability_subtype(getattr(r, "kind_of", None))
    ]
    return GetSimilarNodesResponse(
        session_id=req.session_id,
        candidate_id=req.candidate_id,
        similar=similar,
    )


# ---------------------------------------------------------------------------
# 5. propose_reconciliation
# ---------------------------------------------------------------------------


def _find_existing_graph_matches(
    board_id: str,
    node_candidates: dict,
    embedder,
) -> dict[str, list]:
    """Sync: find existing graph nodes matching session candidates.

    Runs in the thread pool via ``_run_graph_io``.
    """
    from okto_pulse.core.kg.reconciliation import ExistingNodeSummary
    from okto_pulse.core.kg.search import find_similar_for_candidate

    existing_matches: dict[str, list] = {}
    graph_store = get_kg_registry().graph_store
    exact_lookup = getattr(graph_store, "find_active_by_source_ref", None)
    for cand_id, cand in node_candidates.items():
        node_type = (
            cand.node_type.value if hasattr(cand.node_type, "value") else cand.node_type
        )
        matches: list = []
        try:
            query_vec = embedder.encode(f"{cand.title}\n{cand.content or ''}")
            matches = find_similar_for_candidate(
                board_id=board_id,
                node_type=node_type,
                query_vector=query_vec,
                top_k=5,
                min_similarity=0.3,
            )
            matches = [
                match
                for match in matches
                if not is_code_traceability_subtype(getattr(match, "kind_of", None))
            ]
        except Exception as exc:
            logger.warning(
                "kg.primitives.reconciliation_search_failed candidate=%s err=%s",
                cand_id,
                exc,
            )

        # Vector top-k is not an identity index. Decisions need the exact
        # active lineage independently so a low-similarity reversal cannot
        # evade immutable history. Source-backed Entities deliberately retain
        # NC-8's automatic MERGE semantics at commit; the pre-write identity
        # fence validates explicit UPDATE/SUPERSEDE targets independently.
        source_ref = str(cand.source_artifact_ref or "").strip()
        if node_type == "Decision" and source_ref and callable(exact_lookup):
            try:
                exact = exact_lookup(board_id, node_type, source_ref)
            except Exception as exc:
                logger.warning(
                    "kg.primitives.reconciliation_exact_lookup_failed "
                    "candidate=%s ref=%s err=%s",
                    cand_id,
                    source_ref,
                    exc,
                )
            else:
                if exact:
                    exact_summary = ExistingNodeSummary(
                        graph_node_id=str(exact["node_id"]),
                        node_type=str(exact.get("node_type") or node_type),
                        stable_id=exact.get("source_artifact_ref") or source_ref,
                        title=str(exact.get("title") or ""),
                        content=exact.get("content"),
                        context=exact.get("context"),
                        justification=exact.get("justification"),
                        similarity=next(
                            (
                                match.similarity
                                for match in matches
                                if match.graph_node_id == str(exact["node_id"])
                            ),
                            0.0,
                        ),
                    )
                    matches = [
                        exact_summary,
                        *[
                            match
                            for match in matches
                            if match.graph_node_id != exact_summary.graph_node_id
                        ],
                    ]

        if matches:
            existing_matches[cand_id] = matches
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

    async with session.lock:
        _validate_session_state(session, allow_pending_commit=False)
        candidate_snapshot = dict(session.node_candidates)

    _require_code_traceability_candidate_ownership(
        candidate_snapshot,
        agent_id=agent_id,
        session_id=req.session_id,
    )

    if not force_reprocess:
        latest = await _get_latest_audit(
            registry,
            db,
            session.board_id,
            session.artifact_type,
            session.artifact_id,
        )
        nothing_changed = bool(latest and _audit_hash(latest) == session.content_hash)
    else:
        latest = None
        nothing_changed = False

    existing_matches_by_candidate: dict[str, list] = {}
    if not nothing_changed:
        embedder = registry.require_embedding_provider()
        existing_matches_by_candidate = await _run_graph_io(
            _find_existing_graph_matches,
            session.board_id,
            candidate_snapshot,
            embedder,
        )

    hints_by_cid = reconcile_session(
        candidate_snapshot,
        nothing_changed=nothing_changed,
        existing_matches_by_candidate=existing_matches_by_candidate,
    )
    hints = list(hints_by_cid.values())

    async with session.lock:
        _validate_session_state(session, allow_pending_commit=False)
        if session.node_candidates != candidate_snapshot:
            raise KGPrimitiveError(
                "session_mutated_during_reconciliation",
                "Session candidates changed while reconciliation was computed; "
                "retry proposal against the current session snapshot",
                session_id=req.session_id,
            )

        if nothing_changed and not session.count_only_attested:
            await _register_count_only_attestation(
                registry,
                session=session,
                previous_session_id=(_audit_session_id(latest) if latest else None),
                agent_id=agent_id,
                db=db,
            )
        session.reconciliation_hints = hints_by_cid
        session.touch(registry.require_session_store().default_ttl_seconds)

    return ProposeReconciliationResponse(session_id=req.session_id, hints=hints)


# ---------------------------------------------------------------------------
# 6. commit_consolidation
# ---------------------------------------------------------------------------


def _compensate_graph_writes(board_id: str, session_id: str, records: list) -> None:
    """Sync: reverse graph writes for a failed commit.

    Re-open a writer scope and replay the same fail-closed compensation engine
    used inline. Projection receipts, Spec lineage, in-place property
    before-images, session edges and session nodes are restored in that order.
    Any incomplete restore propagates so callers never acknowledge graph-ahead
    state as a successful relational rollback.
    """
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    async def _run() -> None:
        async with await get_kg_registry().graph_transaction.begin(board_id) as scope:
            orchestrator = TransactionOrchestrator(
                graph_scope=scope,
                session_id=session_id,
                board_id=board_id,
            )
            orchestrator.records = list(records)
            await orchestrator.compensate()

    run_async_blocking(_run())


def _connectivity_writer_path(agent_id: str) -> str:
    """Map the runtime caller to the guard's stable writer path vocabulary."""
    if (agent_id or "").startswith("system:"):
        return "deterministic_worker"
    return "commit_consolidation"


def _require_code_traceability_candidate_ownership(
    node_candidates: dict,
    *,
    agent_id: str,
    session_id: str,
) -> None:
    """Fail before staging/provider access when a generic writer forges CT."""

    writer_path = _connectivity_writer_path(agent_id)
    for candidate in node_candidates.values():
        try:
            require_code_traceability_candidate_writer(
                candidate,
                writer_path=writer_path,
            )
        except CodeTraceabilityKGWriteViolation as exc:
            raise KGPrimitiveError(
                "code_traceability_projection_reserved",
                "Code Traceability KG projections are owned by the "
                "deterministic worker",
                session_id=session_id,
                details={
                    "reason": exc.reason,
                    "candidate_id": exc.candidate_id,
                    "reserved_fields": list(exc.reserved_fields),
                    "required_writer_path": (
                        CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH
                    ),
                },
            ) from exc


def _require_no_code_traceability_existing_targets(
    graph_scope,
    *,
    node_candidates: dict,
    effective_hints: dict,
    agent_id: str,
    session_id: str,
) -> None:
    """Prevent generic reconciliation/NC-8 from mutating deterministic CT."""

    if (
        _connectivity_writer_path(agent_id)
        == CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH
    ):
        return

    probes: list[tuple[str, str, str]] = []
    for candidate_id, candidate in node_candidates.items():
        node_type = _enum_value(candidate.node_type)
        if node_type != "Entity":
            continue
        hint = effective_hints.get(candidate_id)
        target_id = str(getattr(hint, "target_node_id", None) or "").strip()
        if target_id:
            probes.append((candidate_id, "id", target_id))
        source_ref = str(candidate.source_artifact_ref or "").strip()
        if source_ref:
            probes.append((candidate_id, "source_artifact_ref", source_ref))

    for candidate_id, lookup_kind, lookup_value in probes:
        if lookup_kind == "id":
            statement = "MATCH (n:Entity {id: $value}) RETURN n.kind_of LIMIT 1"
        else:
            statement = (
                "MATCH (n:Entity {source_artifact_ref: $value}) "
                "WHERE n.superseded_by IS NULL "
                "RETURN n.kind_of LIMIT 1"
            )
        result = graph_scope.execute(statement, {"value": lookup_value})
        if result.rows and is_code_traceability_subtype(result.rows[0][0]):
            raise KGPrimitiveError(
                "code_traceability_projection_reserved",
                "Generic reconciliation cannot update, merge, or supersede "
                "a deterministic Code Traceability projection",
                session_id=session_id,
                details={
                    "candidate_id": candidate_id,
                    "required_writer_path": (
                        CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH
                    ),
                },
            )


def _require_entity_source_identity_matches(
    graph_scope,
    *,
    node_candidates: dict,
    effective_hints: dict,
    session_id: str,
) -> None:
    """Reject cross-source Entity mutation hints before the first write.

    Reconciliation hints and explicit overrides are untrusted planning input.
    A source-backed Entity is the structural root for that exact relational
    artifact identity; UPDATE/SUPERSEDE may reuse its own lineage, but must not
    turn a semantically similar artifact root into its successor. This commit
    boundary also protects stale sessions and forced overrides.
    """

    for candidate_id, candidate in node_candidates.items():
        if _enum_value(candidate.node_type) != "Entity":
            continue
        candidate_ref = str(candidate.source_artifact_ref or "").strip()
        if not candidate_ref:
            continue
        hint = effective_hints.get(candidate_id)
        if hint is None or _resolve_op(hint, candidate.source_confidence) not in {
            ReconciliationOperation.UPDATE,
            ReconciliationOperation.SUPERSEDE,
        }:
            continue
        target_node_id = str(getattr(hint, "target_node_id", None) or "").strip()
        if not target_node_id:
            raise KGPrimitiveError(
                "entity_source_identity_unverifiable",
                "A source-backed Entity mutation requires an existing target "
                "with a verifiable source artifact identity.",
                session_id=session_id,
                details={
                    "candidate_id": candidate_id,
                    "candidate_source_artifact_ref": candidate_ref,
                    "target_node_id": None,
                    "reason": "target_node_id_missing",
                },
            )
        try:
            result = graph_scope.execute(
                "MATCH (n:Entity) WHERE n.id = $id "
                "RETURN n.id, n.source_artifact_ref LIMIT 1",
                {"id": target_node_id},
            )
            rows = getattr(result, "rows", ())
        except Exception as exc:
            raise KGPrimitiveError(
                "entity_source_identity_unverifiable",
                "A source-backed Entity mutation was rejected because its "
                "target identity could not be read.",
                session_id=session_id,
                details={
                    "candidate_id": candidate_id,
                    "candidate_source_artifact_ref": candidate_ref,
                    "target_node_id": target_node_id,
                    "reason": "target_lookup_failed",
                    "failure_type": type(exc).__name__,
                },
            ) from exc
        if not rows:
            raise KGPrimitiveError(
                "entity_source_identity_unverifiable",
                "A source-backed Entity mutation requires an existing target "
                "with a verifiable source artifact identity.",
                session_id=session_id,
                details={
                    "candidate_id": candidate_id,
                    "candidate_source_artifact_ref": candidate_ref,
                    "target_node_id": target_node_id,
                    "reason": "target_not_found",
                },
            )
        target_ref = str(rows[0][1] or "").strip()
        if not target_ref:
            raise KGPrimitiveError(
                "entity_source_identity_unverifiable",
                "A source-backed Entity mutation requires an existing target "
                "with a non-empty source artifact identity.",
                session_id=session_id,
                details={
                    "candidate_id": candidate_id,
                    "candidate_source_artifact_ref": candidate_ref,
                    "target_node_id": target_node_id,
                    "reason": "target_source_artifact_ref_missing",
                },
            )
        if target_ref != candidate_ref:
            raise KGPrimitiveError(
                "entity_source_identity_mismatch",
                "A source-backed Entity cannot update or supersede a different "
                "source artifact identity.",
                session_id=session_id,
                details={
                    "candidate_id": candidate_id,
                    "candidate_source_artifact_ref": candidate_ref,
                    "target_node_id": target_node_id,
                    "target_source_artifact_ref": target_ref,
                },
            )


def _validate_graph_connectivity_before_commit(
    *,
    graph_scope,
    board_id: str,
    session_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    effective_hints: dict,
    explicit_override_candidate_ids: frozenset[str],
    writer_path: str,
    kg_health_state: str,
    deterministic_rdl_alternative_candidate_ids: frozenset[str] = frozenset(),
) -> dict:
    """Validate zero-orphan invariants before any graph backend mutation.

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
        graph_scope=graph_scope,
        node_candidates=node_candidates,
        edge_candidates=edge_candidates,
    )
    _inherit_supersede_provenance_edges(
        graph_scope=graph_scope,
        board_id=board_id,
        node_candidates=node_candidates,
        edge_candidates=edge_candidates,
        effective_hints=effective_hints,
        explicit_override_candidate_ids=explicit_override_candidate_ids,
    )
    guard_nodes: list[object] = []
    guard_edges: list[object] = list(edge_candidates.values())
    existing_refs: list[KGNodeRef] = []
    candidate_to_existing_id: dict[str, str] = {}

    for cand_id, cand in node_candidates.items():
        node_type = _enum_value(cand.node_type)
        op = _resolve_op(effective_hints.get(cand_id), cand.source_confidence)
        existing_id = _planned_existing_node_id(
            graph_scope=graph_scope,
            cand=cand,
            node_type=node_type,
            op=op,
            hint=effective_hints.get(cand_id),
            has_explicit_override=cand_id in explicit_override_candidate_ids,
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
                    graph_scope=graph_scope,
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
            and not (
                _node_is_human_curated(
                    graph_scope,
                    node_type,
                    effective_hints[cand_id].target_node_id,
                )
                and cand_id not in explicit_override_candidate_ids
            )
        ):
            target_id = effective_hints[cand_id].target_node_id
            target_type = _lookup_node_type_by_id(graph_scope, target_id) or node_type
            guard_edges.append(
                {
                    "candidate_id": f"{cand_id}__supersedes_existing",
                    "edge_type": "supersedes",
                    "from_candidate_id": cand_id,
                    "to_candidate_id": f"kg:{target_id}",
                }
            )
            existing_refs.append(
                KGNodeRef(
                    ref_id=f"kg:{target_id}",
                    node_type=target_type,
                    source_artifact_ref=None,
                )
            )

    existing_refs.extend(
        _existing_refs_for_edge_endpoints(
            graph_scope=graph_scope,
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
        deterministic_rdl_alternative_candidate_ids=(
            deterministic_rdl_alternative_candidate_ids
        ),
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


def _validated_deterministic_rdl_alternative_grants(
    *,
    agent_id: str,
    session_artifact_type: str,
    session_artifact_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    relational_projection_candidate_ids: frozenset[str],
    relational_projection_active_set_intent: object | None,
) -> frozenset[str]:
    """Prove the narrow server-owned RDL Alternative writer exception.

    ``Alternative`` remains cognitive-owned in the generic registry.  The one
    deterministic producer is the current relational RDL projection, already
    admitted by ``begin_consolidation``.  Re-prove its closed identities and
    provenance at the graph boundary so an arbitrary deterministic candidate,
    prefix collision, or mutable intent cannot inherit that authority.
    """

    intent = relational_projection_active_set_intent
    if (
        type(node_candidates) is not dict
        or type(edge_candidates) is not dict
        or type(relational_projection_candidate_ids) is not frozenset
        or any(
            type(candidate_id) is not str or not candidate_id
            for candidate_id in relational_projection_candidate_ids
        )
        or any(
            type(candidate_id) is not str
            or not candidate_id
            or str(getattr(candidate, "candidate_id", "")) != candidate_id
            for candidate_id, candidate in node_candidates.items()
        )
        or any(
            type(candidate_id) is not str
            or not candidate_id
            or str(getattr(candidate, "candidate_id", "")) != candidate_id
            for candidate_id, candidate in edge_candidates.items()
        )
        or agent_id != "system:historical_consolidation"
        or session_artifact_type != "refinement"
        or intent is None
        or type(getattr(intent, "owner_type", None)) is not str
        or type(getattr(intent, "owner_id", None)) is not str
        or type(getattr(intent, "namespace", None)) is not str
        or getattr(intent, "owner_type") != "refinement"
        or getattr(intent, "owner_id") != session_artifact_id
        or getattr(intent, "namespace") != "rdl"
        or type(getattr(intent, "active_refs", None)) is not tuple
        or type(getattr(intent, "active_edges", None)) is not tuple
        or getattr(intent, "active_edges")
    ):
        return frozenset()

    active_refs = getattr(intent, "active_refs")
    refs_by_candidate: dict[str, object] = {}
    parsed_by_candidate: dict[str, object] = {}
    for ref in active_refs:
        candidate_id = getattr(ref, "candidate_id", None)
        node_type = getattr(ref, "node_type", None)
        source_ref = getattr(ref, "source_artifact_ref", None)
        if (
            type(candidate_id) is not str
            or not candidate_id
            or candidate_id in refs_by_candidate
            or type(node_type) is not str
            or type(source_ref) is not str
        ):
            return frozenset()
        parsed = parse_relational_projection_ref(source_ref)
        candidate = node_candidates.get(candidate_id)
        if (
            parsed is None
            or parsed.owner_id != session_artifact_id
            or parsed.owner_type != "refinement"
            or parsed.namespace != "rdl"
            or parsed.node_type != node_type
            or relational_projection_candidate_id(source_ref) != candidate_id
            or candidate is None
            or _enum_value(getattr(candidate, "node_type", "")) != node_type
            or str(getattr(candidate, "source_artifact_ref", "") or "") != source_ref
        ):
            return frozenset()
        refs_by_candidate[candidate_id] = ref
        parsed_by_candidate[candidate_id] = parsed

    if frozenset(refs_by_candidate) != relational_projection_candidate_ids:
        return frozenset()

    owner_candidates = [
        candidate_id
        for candidate_id, candidate in node_candidates.items()
        if _enum_value(getattr(candidate, "node_type", "")) == "Entity"
        and str(getattr(candidate, "source_artifact_ref", "") or "")
        == f"refinement:{session_artifact_id}"
    ]
    if len(owner_candidates) != 1:
        return frozenset()
    owner_candidate_id = owner_candidates[0]

    def _edges(*, edge_type: str, from_id: str, to_id: str) -> list[object]:
        return [
            edge
            for edge in edge_candidates.values()
            if _enum_value(getattr(edge, "edge_type", "")) == edge_type
            and str(getattr(edge, "from_candidate_id", "")) == from_id
            and str(getattr(edge, "to_candidate_id", "")) == to_id
        ]

    decision_ids = [
        candidate_id
        for candidate_id, parsed in parsed_by_candidate.items()
        if getattr(parsed, "node_type") == "Decision"
    ]
    for decision_id in decision_ids:
        decision_belongs = _edges(
            edge_type="belongs_to",
            from_id=decision_id,
            to_id=owner_candidate_id,
        )
        all_outgoing_belongs = [
            edge
            for edge in edge_candidates.values()
            if _enum_value(getattr(edge, "edge_type", "")) == "belongs_to"
            and str(getattr(edge, "from_candidate_id", "")) == decision_id
        ]
        if len(decision_belongs) != 1 or len(all_outgoing_belongs) != 1:
            return frozenset()
        belongs = decision_belongs[0]
        if (
            str(getattr(belongs, "candidate_id", ""))
            != relational_projection_edge_id(
                "belongs_to",
                decision_id,
                owner_candidate_id,
            )
            or str(getattr(belongs, "rule_id", "") or "")
            != relational_projection_belongs_to_rule("Decision")
            or str(getattr(belongs, "layer", "") or "") != "deterministic"
            or str(getattr(belongs, "created_by", "") or "") != "worker_layer1"
        ):
            return frozenset()

    granted: set[str] = set()
    for alternative_id, parsed in parsed_by_candidate.items():
        if getattr(parsed, "node_type") != "Alternative":
            continue
        belongs_edges = _edges(
            edge_type="belongs_to",
            from_id=alternative_id,
            to_id=owner_candidate_id,
        )
        all_outgoing_belongs = [
            edge
            for edge in edge_candidates.values()
            if _enum_value(getattr(edge, "edge_type", "")) == "belongs_to"
            and str(getattr(edge, "from_candidate_id", "")) == alternative_id
        ]
        if len(belongs_edges) != 1 or len(all_outgoing_belongs) != 1:
            return frozenset()
        belongs = belongs_edges[0]
        if (
            str(getattr(belongs, "candidate_id", ""))
            != relational_projection_edge_id(
                "belongs_to",
                alternative_id,
                owner_candidate_id,
            )
            or str(getattr(belongs, "rule_id", "") or "")
            != relational_projection_belongs_to_rule("Alternative")
            or str(getattr(belongs, "layer", "") or "") != "deterministic"
            or str(getattr(belongs, "created_by", "") or "") != "worker_layer1"
        ):
            return frozenset()

        ledger_id = getattr(parsed, "ledger_id")
        decision_ids = [
            candidate_id
            for candidate_id, decision_parsed in parsed_by_candidate.items()
            if getattr(decision_parsed, "node_type") == "Decision"
            and getattr(decision_parsed, "ledger_id") == ledger_id
        ]
        if len(decision_ids) != 1:
            return frozenset()
        decision_id = decision_ids[0]
        relation_edges = _edges(
            edge_type="relates_to",
            from_id=decision_id,
            to_id=alternative_id,
        )
        all_incoming_relations = [
            edge
            for edge in edge_candidates.values()
            if _enum_value(getattr(edge, "edge_type", "")) == "relates_to"
            and str(getattr(edge, "to_candidate_id", "")) == alternative_id
        ]
        if len(relation_edges) != 1 or len(all_incoming_relations) != 1:
            return frozenset()
        relation = relation_edges[0]
        if (
            str(getattr(relation, "candidate_id", ""))
            != relational_projection_edge_id(
                "relates_to",
                decision_id,
                alternative_id,
            )
            or str(getattr(relation, "rule_id", "") or "")
            != relational_projection_alternative_relation_rule()
            or str(getattr(relation, "layer", "") or "") != "deterministic"
            or str(getattr(relation, "created_by", "") or "") != "worker_layer1"
        ):
            return frozenset()
        granted.add(alternative_id)
    return frozenset(granted)


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
    graph_scope,
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
        root = _resolve_provenance_root(graph_scope, source_ref)
        if root is None:
            continue
        root_node_id, root_node_type = root
        cand_node_type = _enum_value(getattr(cand, "node_type", ""))
        candidate_existing_id = _lookup_existing_node(
            graph_scope,
            cand_node_type,
            source_ref,
        )
        if candidate_existing_id == root_node_id and cand_node_type == root_node_type:
            logger.warning(
                "kg.connectivity.auto_provenance_self_loop_skipped "
                "candidate=%s node=%s type=%s",
                cand_id,
                root_node_id,
                root_node_type,
                extra={
                    "event": "kg.connectivity.auto_provenance_self_loop_skipped",
                    "candidate_id": cand_id,
                    "node_id": root_node_id,
                    "node_type": root_node_type,
                },
            )
            continue
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
            fallback_reason=(f"auto_attached_to_{root_node_type.lower()}_source_root"),
        )


def _inherit_supersede_provenance_edges(
    *,
    graph_scope,
    board_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    effective_hints: dict,
    explicit_override_candidate_ids: frozenset[str],
) -> None:
    """Carry a predecessor's proven ``belongs_to`` edge into its successor.

    A SUPERSEDE creates a fresh node. Reusing the predecessor's edge only as
    guard evidence would acknowledge an orphan successor, while an at-least-once
    replay would then fail because the active successor has no provenance edge.
    Add the bounded edge to the materializable batch instead. The inheritance is
    fail-closed: source refs and node types must match, curated targets still
    require an explicit override, and any conflicting deterministic successor
    leaves the normal connectivity rejection in place.
    """

    for cand_id, cand in list(node_candidates.items()):
        if _has_outgoing_edge(edge_candidates, cand_id, "belongs_to"):
            continue

        hint = effective_hints.get(cand_id)
        if (
            _resolve_op(hint, cand.source_confidence)
            is not ReconciliationOperation.SUPERSEDE
            or hint is None
            or not getattr(hint, "target_node_id", None)
        ):
            continue

        node_type = _enum_value(cand.node_type)
        target_node_id = str(hint.target_node_id)
        if _lookup_node_type_by_id(graph_scope, target_node_id) != node_type:
            continue
        if (
            _node_is_human_curated(graph_scope, node_type, target_node_id)
            and cand_id not in explicit_override_candidate_ids
        ):
            continue

        source_ref = str(cand.source_artifact_ref or "").strip()
        target_source_ref = str(
            _lookup_node_source_ref_by_id(
                graph_scope,
                node_type,
                target_node_id,
            )
            or ""
        ).strip()
        if not source_ref or target_source_ref != source_ref:
            continue

        successor_generation = (
            _node_generation(graph_scope, node_type, target_node_id) + 1
        )
        successor_id = mint_node_id(
            board_id,
            node_type,
            derive_natural_key(source_ref, node_type, cand.title),
            successor_generation,
        )
        existing_successor = _lookup_existing_node_identity_by_id(
            graph_scope,
            node_type,
            successor_id,
        )
        if existing_successor is not None:
            successor_source_ref = str(
                existing_successor.get("source_artifact_ref") or ""
            ).strip()
            linked_successor = _node_superseded_by(
                graph_scope,
                node_type,
                target_node_id,
            )
            if (
                successor_source_ref and successor_source_ref != source_ref
            ) or linked_successor not in (None, successor_id):
                continue

        match = _find_existing_connectivity_match(
            graph_scope=graph_scope,
            node_type=node_type,
            node_id=target_node_id,
            edge_type="belongs_to",
            direction="outgoing",
            target_node_types=("Entity", "Bug"),
        )
        if match is None:
            continue
        parent_id, parent_type, direction, _parent_layer = match
        if direction != "outgoing" or (
            node_type,
            parent_type,
        ) not in _allowed_edge_pairs("belongs_to"):
            continue

        edge_id = f"{cand_id}__inherit_supersede_belongs_to"
        edge_candidates[edge_id] = EdgeCandidate(
            candidate_id=edge_id,
            edge_type=KGEdgeType.BELONGS_TO,
            from_candidate_id=cand_id,
            to_candidate_id=f"kg:{parent_id}",
            confidence=1.0,
            layer="deterministic",
            rule_id="belongs_to/inherit_supersede@commit_consolidation",
            created_by="system:commit_consolidation",
            fallback_reason=(
                "preserve_predecessor_provenance_on_deterministic_supersede"
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


def _resolve_provenance_root(graph_scope, source_ref: str) -> tuple[str, str] | None:
    for root_ref in _source_root_ref_candidates(source_ref):
        for node_type in ("Entity", "Bug"):
            node_id = _lookup_existing_node(graph_scope, node_type, root_ref)
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
    if kg_health_state not in DEGRADED_KG_STATES:
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
    graph_scope,
    cand,
    node_type: str,
    op: ReconciliationOperation,
    hint: ReconciliationHint | None,
    has_explicit_override: bool,
) -> str | None:
    source_ref = cand.source_artifact_ref or ""
    if source_ref:
        existing_by_source = _lookup_existing_node(graph_scope, node_type, source_ref)
        if existing_by_source:
            return existing_by_source

    if op == ReconciliationOperation.NOOP:
        return None

    target_node_id = getattr(hint, "target_node_id", None) if hint else None
    if (
        op
        in (
            ReconciliationOperation.UPDATE,
            ReconciliationOperation.SUPERSEDE,
        )
        and target_node_id
    ):
        is_curated = _node_is_human_curated(graph_scope, node_type, target_node_id)
        if is_curated and has_explicit_override:
            return None
        if op == ReconciliationOperation.UPDATE or is_curated:
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
    graph_scope,
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
                endpoint,
                candidate_to_existing_id,
                graph_scope=graph_scope,
            )
            if not node_id:
                continue
            node_type = resolved_type or _lookup_node_type_by_id(graph_scope, node_id)
            if not node_type:
                continue
            # R7: carry the endpoint's graph_layer so the layer-aware guard can
            # tell a canonical Bug from a working one for explicit edge endpoints.
            node_layer = _lookup_node_layer_by_id(graph_scope, node_type, node_id)
            # RKG-02: also carry the real source_artifact_ref so the guard's
            # type-aware canonical-bug probe can reconcile a Learning's
            # card:<uuid> with this existing Bug endpoint.
            node_source_ref = _lookup_node_source_ref_by_id(
                graph_scope, node_type, node_id
            )
            refs.append(
                KGNodeRef(
                    ref_id=endpoint,
                    node_type=node_type,
                    graph_layer=node_layer,
                    source_artifact_ref=node_source_ref,
                )
            )
            refs.append(
                KGNodeRef(
                    ref_id=node_id,
                    node_type=node_type,
                    graph_layer=node_layer,
                    source_artifact_ref=node_source_ref,
                )
            )
            refs.append(
                KGNodeRef(
                    ref_id=f"kg:{node_id}",
                    node_type=node_type,
                    graph_layer=node_layer,
                    source_artifact_ref=node_source_ref,
                )
            )
    return refs


def _existing_connectivity_edges_for_candidate(
    *,
    graph_scope,
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

    bug_probe = _graph_canonical_bug_probe(graph_scope)
    if node_type == "Learning" and _candidate_has_known_bug_source(cand, bug_probe):
        from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL

        # Mirrors connectivity_guard._learning_bug_group: a bug-derived canonical
        # Learning only reaches completeness through a *canonical* Bug (R7).
        groups = [
            KGConnectivityEdgeGroup(
                name="bug_learning",
                alternatives=(
                    KGConnectivityEdgeRequirement(
                        "validates",
                        "outgoing",
                        ("Bug",),
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
                    graph_scope=graph_scope,
                    node_type=node_type,
                    node_id=node_id,
                    edge_type=req.edge_type,
                    direction=req.direction,
                    target_node_types=req.target_node_types,
                    required_target_layer=req.required_target_layer,
                )
            if match is None:
                match = _find_existing_connectivity_match(
                    graph_scope=graph_scope,
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
            other_source_ref = _lookup_node_source_ref_by_id(
                graph_scope, other_type, other_id
            )
            existing_refs.append(
                KGNodeRef(
                    ref_id=endpoint_ref,
                    node_type=other_type,
                    graph_layer=other_layer,
                    source_artifact_ref=other_source_ref,
                )
            )
            existing_refs.append(
                KGNodeRef(
                    ref_id=other_id,
                    node_type=other_type,
                    graph_layer=other_layer,
                    source_artifact_ref=other_source_ref,
                )
            )
            if direction == "outgoing":
                from_ref = candidate_id
                to_ref = endpoint_ref
            else:
                from_ref = endpoint_ref
                to_ref = candidate_id
            synthesized.append(
                {
                    "candidate_id": f"{candidate_id}__existing_{req.edge_type}",
                    "edge_type": req.edge_type,
                    "from_candidate_id": from_ref,
                    "to_candidate_id": to_ref,
                }
            )
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
    from okto_pulse.core.kg.cognitive_source_ref_resolver import (
        resolve_cognitive_source_ref,
    )

    source_ref = getattr(cand, "source_artifact_ref", "") or ""
    return resolve_cognitive_source_ref(
        source_ref, canonical_bug_probe=bug_probe
    ).is_bug_derived


def _find_existing_connectivity_match(
    *,
    graph_scope,
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
                res = graph_scope.execute(cypher, params)
                if res.rows:
                    row = res.rows[0]
                    return row[0], target_type, actual_direction, row[1]
            except Exception:
                continue
    return None


def _lookup_node_layer_by_id(graph_scope, node_type: str, node_id: str) -> str | None:
    """Return a node's graph_layer (or None if unknown/unreadable).

    None is fail-closed downstream: the R7 layer-aware guard treats an
    unresolved layer as non-canonical.
    """
    if not node_id or not node_type:
        return None
    try:
        res = graph_scope.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.graph_layer LIMIT 1",
            {"id": node_id},
        )
        if res.rows:
            value = res.rows[0][0]
            return str(value) if value is not None else None
    except Exception:
        return None
    return None


def _lookup_node_source_ref_by_id(
    graph_scope, node_type: str, node_id: str
) -> str | None:
    """Return a node's source_artifact_ref (or None). RKG-02: the connectivity
    guard's type-aware canonical-bug probe needs the real Bug source_ref of an
    EXISTING endpoint to reconcile a Learning's card:<uuid> with the canonical
    Bug; the worker must load it instead of leaving it None."""
    if not node_id or not node_type:
        return None
    try:
        res = graph_scope.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.source_artifact_ref LIMIT 1",
            {"id": node_id},
        )
        if res.rows:
            value = res.rows[0][0]
            return str(value) if value is not None else None
    except Exception:
        return None
    return None


def _graph_canonical_bug_probe(graph_scope):
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
            res = graph_scope.execute(
                "MATCH (b:Bug) WHERE b.graph_layer = 'canonical' "
                "RETURN b.id, b.source_artifact_ref",
                {},
            )
            for bid, bsref in res.rows:
                if bid:
                    keys.add(normalize_cognitive_artifact_id(f"card:{bid}"))
                if bsref:
                    keys.add(
                        normalize_cognitive_artifact_id(
                            strip_concept_suffix(str(bsref))
                        )
                    )
        except Exception:
            pass

    def _probe(uuid: str) -> bool:
        _ensure_loaded()
        return normalize_cognitive_artifact_id(f"card:{uuid}") in keys

    return _probe


def _lookup_node_type_by_id(graph_scope, node_id: str) -> str | None:
    if not node_id:
        return None
    for node_type in _kg_node_types():
        try:
            res = graph_scope.execute(
                f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.id LIMIT 1",
                {"id": node_id},
            )
            if res.rows:
                return node_type
        except Exception:
            continue
    return None


def _kg_node_types() -> tuple[str, ...]:
    from okto_pulse.core.kg.schema_contract import NODE_TYPES

    return tuple(NODE_TYPES)


# Estado efetivo de health por board para o write-path, com TTL curto.
# Somente estados que BLOQUEIAM escrita podem ser reutilizados: armazenar
# ``healthy`` aqui criaria uma janela de autorização obsoleta capaz de
# ocultar uma falha/quarentena recém-detectada pelo health reader.
_COMMIT_HEALTH_CACHE = runtime_state("kg.primitives.commit_health_cache", dict)
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

    ``db is None`` is the explicit compatibility lane for internal staging
    and direct graph-harness calls that have no relational health reader. It
    is not cached and never authorizes a later production commit: MCP/UoW
    commit supplies its real relational context and re-runs this resolver
    immediately before dispatching the graph callback.

    Catch-22 fix (2026-06-10): ``recovery_needed`` bloqueava TODA mutação —
    inclusive a re-materialização de um grafo vazio (única cura de
    ``empty_after_materialized_history``; 994 entries foram para a DLQ em
    campo) e os retries após falha TRANSITÓRIA de escrita (ex.: buffer
    manager exausto), que re-registrava ``kg.commit.failed`` e realimentava o
    próprio estado. Falha recente de escrita não implica grafo corrompido.

    Regra efetiva para o WRITE-PATH:
    - ``quarantined`` → bloqueia sempre.
    - ``recovery_needed`` explicitamente atribuído ao ``graph_state``, com
      ``overall_state`` compatível, ``discovery_state`` não mais severo e
      grafo LEGÍVEL (o health acabou de contar os nodes — ``total_nodes``
      presente, 0 ou N) → permite a mutação: a conectividade real é validada
      contra o grafo aberto, e um commit bem-sucedido limpa as falhas de
      write do ring buffer (self-heal).
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
    if cached is not None:
        cached_state = str(cached[1])
        if (
            cached_state in DEGRADED_KG_STATES
            and now - cached[0] < _COMMIT_HEALTH_CACHE_TTL_S
        ):
            return cached_state
        # A runtime reload may leave an entry produced by an older version
        # that cached permissive states.  Never let it authorize this call.
        _COMMIT_HEALTH_CACHE.pop(board_id, None)

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
        state = "recovery_needed"
        _COMMIT_HEALTH_CACHE[board_id] = (now, state)
        return state
    state_severity = {
        "healthy": 0,
        "at_risk": 1,
        "backpressure": 2,
        "recovery_needed": 3,
        "quarantined": 4,
    }
    state_fields = ("overall_state", "graph_state", "discovery_state")
    resolved_states: dict[str, str] = {}
    invalid_fields: list[str] = []
    if isinstance(health, dict):
        for field in state_fields:
            raw_value = health.get(field)
            if isinstance(raw_value, str) and raw_value in state_severity:
                resolved_states[field] = raw_value
            else:
                invalid_fields.append(field)
    else:
        invalid_fields.extend(state_fields)

    if invalid_fields:
        logger.warning(
            "kg.connectivity.health_payload_invalid board=%s fields=%s",
            board_id,
            ",".join(invalid_fields),
            extra={
                "event": "kg.connectivity.health_payload_invalid",
                "board_id": board_id,
                "invalid_fields": tuple(invalid_fields),
            },
        )
        # A known quarantine remains authoritative even when another field is
        # malformed. Otherwise fail closed to recovery_needed.
        state = (
            "quarantined"
            if "quarantined" in resolved_states.values()
            else "recovery_needed"
        )
    else:
        # The health contract is worst-case-wins. Never trust a contradictory
        # ``overall_state`` that hides a more severe board/discovery state.
        state = max(
            resolved_states.values(),
            key=state_severity.__getitem__,
        )
        if resolved_states["overall_state"] != state:
            logger.warning(
                "kg.connectivity.health_payload_inconsistent board=%s "
                "overall=%s effective=%s",
                board_id,
                resolved_states["overall_state"],
                state,
                extra={
                    "event": "kg.connectivity.health_payload_inconsistent",
                    "board_id": board_id,
                    "overall_state": resolved_states["overall_state"],
                    "effective_state": state,
                },
            )

    raw_total_nodes = health.get("total_nodes") if isinstance(health, dict) else None
    if (
        state == "recovery_needed"
        and not invalid_fields
        and resolved_states["overall_state"] == "recovery_needed"
        and resolved_states["graph_state"] == "recovery_needed"
        and (
            state_severity[resolved_states["discovery_state"]]
            <= state_severity["recovery_needed"]
        )
        and raw_total_nodes is not None
    ):
        # total_nodes AUSENTE = payload sem telemetria de contagem (grafo
        # ilegível) → mantém o bloqueio (conservador, contrato Zero-Orphan).
        total_nodes = (
            raw_total_nodes
            if isinstance(raw_total_nodes, int)
            and not isinstance(raw_total_nodes, bool)
            else -1
        )
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

    if state in DEGRADED_KG_STATES:
        _COMMIT_HEALTH_CACHE[board_id] = (now, state)
    else:
        _COMMIT_HEALTH_CACHE.pop(board_id, None)
    return state


def _preserve_decision_history_for_updates(
    *,
    graph_scope,
    node_candidates: dict,
    effective_hints: dict,
) -> dict:
    """Convert semantic Decision UPDATEs into lineage-preserving SUPERSEDEs."""

    from okto_pulse.core.kg.reconciliation import decision_semantics_differ

    guarded = dict(effective_hints)
    for candidate_id, candidate in node_candidates.items():
        node_type = _enum_value(candidate.node_type)
        hint = guarded.get(candidate_id)
        if (
            node_type != "Decision"
            or hint is None
            or _resolve_op(hint, candidate.source_confidence)
            != ReconciliationOperation.UPDATE
            or not getattr(hint, "target_node_id", None)
        ):
            continue

        target_node_id = hint.target_node_id
        existing = _node_semantic_fields(
            graph_scope,
            node_type,
            target_node_id,
        )
        if existing is not None and not decision_semantics_differ(
            candidate,
            existing,
        ):
            continue

        guarded[candidate_id] = ReconciliationHint(
            candidate_id=candidate_id,
            operation=ReconciliationOperation.SUPERSEDE,
            target_node_id=target_node_id,
            confidence=hint.confidence,
            reason=(
                "Commit invariant converted a semantic Decision UPDATE into "
                "SUPERSEDE so the prior generation and its edges remain "
                "auditable"
            ),
        )
        logger.warning(
            "kg.consolidation.decision_update_converted candidate=%s "
            "target=%s semantics_readable=%s",
            candidate_id,
            target_node_id,
            existing is not None,
            extra={
                "event": "kg.consolidation.decision_update_converted",
                "candidate_id": candidate_id,
                "target_node_id": target_node_id,
                "semantics_readable": existing is not None,
            },
        )
    return guarded


_SEMANTIC_PROJECTION_NODE_ATTRS: tuple[str, ...] = (
    "kind_of",
    "investigation_receipt_id",
    "source_ref",
    "attestor_actor_id",
    "declared_revision",
    "workspace_state_id",
    "code_path",
    "symbol_qualified_name",
    "symbol_kind",
    "selector_kind",
    "selector_fingerprint",
    "resolution_state",
)


def _semantic_projection_attrs(candidate: object) -> dict[str, object | None]:
    """Copy only schema-declared semantic projection metadata.

    Code Traceability candidates originate in persisted Pulse rows.  Keeping
    this allowlist beside the graph commit boundary prevents an arbitrary
    adapter attribute (including snippets or transport secrets) from leaking
    into node properties.
    """

    return {
        name: getattr(candidate, name, None) for name in _SEMANTIC_PROJECTION_NODE_ATTRS
    }


def _do_graph_commit(
    board_id: str,
    session_id: str,
    node_candidates: dict,
    edge_candidates: dict,
    effective_hints: dict,
    agent_id: str,
    embedder,
    kg_health_state: str,
    session_content_hash: str = "",
    session_artifact_id: str = "",
    explicit_override_candidate_ids: frozenset[str] = frozenset(),
    session_artifact_type: str = "",
    spec_lineage_parent_intent: SpecLineageParentIntent = (
        SpecLineageParentIntent.PRESERVE
    ),
    relational_projection_candidate_ids: frozenset[str] = frozenset(),
    relational_projection_active_set_intent: object | None = None,
) -> tuple[dict, object, list, datetime, dict, list[dict]]:
    """Synchronous graph writes for ``commit_consolidation``.

    Runs in the thread pool via ``_run_graph_io``. Returns
    ``(candidate_to_graph_id, counters, records, committed_at, connectivity,
    cognitive_source_records)`` on success.  The durable records are returned
    to the async coordinator and are never written while this function owns
    the process-global embedded graph writer.
    Raises ``KGPrimitiveError`` on failure (after inline compensation).
    """
    from okto_pulse.core.kg.transaction import TransactionOrchestrator
    from okto_pulse.core.kg.reconciliation import decision_semantics_differ

    try:
        lineage_intent = SpecLineageParentIntent(spec_lineage_parent_intent)
    except ValueError as exc:
        raise KGPrimitiveError(
            "spec_lineage_parent_intent_invalid",
            "Unknown Spec-lineage parent intent.",
            session_id=session_id,
        ) from exc
    clear_source_candidate_id = _validate_spec_lineage_parent_intent(
        intent=lineage_intent,
        artifact_type=session_artifact_type,
        artifact_id=session_artifact_id,
        agent_id=agent_id,
        node_candidates=node_candidates,
        edge_candidates=edge_candidates,
        session_id=session_id,
        force_reprocess=True,
    )

    writer_path = _connectivity_writer_path(agent_id)
    _validate_degraded_connectivity_before_open(
        board_id=board_id,
        session_id=session_id,
        node_candidates=node_candidates,
        edge_candidates=edge_candidates,
        writer_path=writer_path,
        kg_health_state=kg_health_state,
    )

    graph_scope = run_async_blocking(
        get_kg_registry().graph_transaction.begin(board_id)
    )
    orch = TransactionOrchestrator(
        graph_scope=graph_scope,
        # SQLite writes happen in async context
        session_id=session_id,
        board_id=board_id,
    )
    candidate_to_graph_id: dict[str, str] = {}
    candidate_to_node_type: dict[str, str] = {}
    # Spec MKG-A-S1 (FR4): durable-source records collected for cognitive
    # nodes (Decision/Learning/Alternative/Assumption) written by this commit.
    cognitive_source_records: list[dict] = []

    def _queue_cognitive_source_record(
        *,
        node_id: str,
        node_type: str,
        generation: int,
        attrs: dict,
    ) -> None:
        """Queue one MKG-A append while keeping identity inputs explicit."""

        if node_type not in _COGNITIVE_SOURCE_TYPES:
            return
        from okto_pulse.core.kg.relational_projection import (
            is_relational_projection_node,
        )

        if is_relational_projection_node(
            node_type=node_type,
            source_artifact_ref=str(attrs.get("source_artifact_ref") or ""),
            created_by_agent=str(attrs.get("created_by_agent") or ""),
        ):
            # Relational RDL projections are rebuildable derivatives. Persisting
            # them in MKG-A would create a second source of truth.
            return
        cognitive_source_records.append(
            _cognitive_source_record_kwargs(
                board_id=board_id,
                session_id=session_id,
                node_id=node_id,
                node_type=node_type,
                generation=generation,
                attrs=attrs,
            )
        )

    try:
        _require_no_code_traceability_existing_targets(
            graph_scope,
            node_candidates=node_candidates,
            effective_hints=effective_hints,
            agent_id=agent_id,
            session_id=session_id,
        )
        effective_hints = _preserve_decision_history_for_updates(
            graph_scope=graph_scope,
            node_candidates=node_candidates,
            effective_hints=effective_hints,
        )
        # Generic Decision reconciliation upgrades semantic UPDATEs to
        # SUPERSEDE so cognitive history remains walkable.  RDL history is
        # already immutable in the relational ledger, while its KG nodes are
        # current-state projections.  Normalize those admitted candidates
        # back to UPDATE before the connectivity guard and write loop so no
        # synthetic supersedes edge or duplicate graph node is introduced.
        effective_hints = dict(effective_hints)
        for projection_candidate_id in relational_projection_candidate_ids:
            projection_hint = effective_hints.get(projection_candidate_id)
            if (
                projection_hint is not None
                and _resolve_op(projection_hint, 1.0)
                == ReconciliationOperation.SUPERSEDE
                and getattr(projection_hint, "target_node_id", None)
            ):
                effective_hints[projection_candidate_id] = projection_hint.model_copy(
                    update={"operation": ReconciliationOperation.UPDATE}
                )
        _require_entity_source_identity_matches(
            graph_scope,
            node_candidates=node_candidates,
            effective_hints=effective_hints,
            session_id=session_id,
        )
        resolved_dependency_endpoints = _resolve_spec_dependency_endpoints(
            projection_intent=relational_projection_active_set_intent,
            edge_candidates=edge_candidates,
            session_id=session_id,
            graph_scope=graph_scope,
        )
        deterministic_rdl_alternative_grants = (
            _validated_deterministic_rdl_alternative_grants(
                agent_id=agent_id,
                session_artifact_type=session_artifact_type,
                session_artifact_id=session_artifact_id,
                node_candidates=node_candidates,
                edge_candidates=edge_candidates,
                relational_projection_candidate_ids=(
                    relational_projection_candidate_ids
                ),
                relational_projection_active_set_intent=(
                    relational_projection_active_set_intent
                ),
            )
        )
        connectivity = _validate_graph_connectivity_before_commit(
            graph_scope=graph_scope,
            board_id=board_id,
            session_id=session_id,
            node_candidates=node_candidates,
            edge_candidates=edge_candidates,
            effective_hints=effective_hints,
            explicit_override_candidate_ids=explicit_override_candidate_ids,
            writer_path=writer_path,
            kg_health_state=kg_health_state,
            deterministic_rdl_alternative_candidate_ids=(
                deterministic_rdl_alternative_grants
            ),
        )
        for endpoint, (node_id, node_type) in resolved_dependency_endpoints.items():
            candidate_to_graph_id[endpoint] = node_id
            candidate_to_node_type[endpoint] = node_type
        for cand_id, cand in node_candidates.items():
            hint = effective_hints.get(cand_id)
            op = _resolve_op(hint, cand.source_confidence)
            node_type = _enum_value(cand.node_type)
            is_relational_projection = cand_id in relational_projection_candidate_ids

            if op == ReconciliationOperation.NOOP:
                # Spec eca49df9 (FR6): NOOP is a processed candidate too.
                orch.counters.nodes_noop += 1
                existing_id = _lookup_existing_node(
                    graph_scope, node_type, cand.source_artifact_ref or ""
                )
                if existing_id:
                    candidate_to_graph_id[cand_id] = existing_id
                    candidate_to_node_type[cand_id] = node_type
                continue

            # Spec 4007e4a3 (Ideação #3, BR4 + BR5): automatic reconciliation
            # must preserve nodes explicitly curated by a human. The only
            # trustworthy override signal is membership in the request's
            # agent_overrides map; hint confidence is evidence strength and
            # must never double as authorization.
            is_curated_target = False
            has_explicit_override = cand_id in explicit_override_candidate_ids
            if (
                op
                in (
                    ReconciliationOperation.UPDATE,
                    ReconciliationOperation.SUPERSEDE,
                )
                and hint
                and getattr(hint, "target_node_id", None)
            ):
                target_node_id = hint.target_node_id
                is_curated_target = _node_is_human_curated(
                    graph_scope,
                    node_type,
                    target_node_id,
                )
                # A relational RDL projection is rebuildable current state,
                # never a user-curated cognitive source.  Relational truth
                # therefore wins even if a stale/corrupt graph row retained
                # the generic human_curated marker.
                if is_relational_projection:
                    is_curated_target = False
                if is_curated_target and not has_explicit_override:
                    logger.info(
                        "kg.consolidation.manual_edit_preserved candidate=%s "
                        "target_node_id=%s session=%s",
                        cand_id,
                        target_node_id,
                        session_id,
                        extra={
                            "event": "kg.consolidation.manual_edit_preserved",
                            "candidate_id": cand_id,
                            "target_node_id": target_node_id,
                            "session_id": session_id,
                            "requested_operation": op.value,
                        },
                    )
                    orch.counters.nodes_noop += 1
                    candidate_to_graph_id[cand_id] = target_node_id
                    candidate_to_node_type[cand_id] = node_type
                    continue
                if is_curated_target:
                    logger.info(
                        "kg.consolidation.reset_manual_flag candidate=%s "
                        "target_node_id=%s session=%s",
                        cand_id,
                        target_node_id,
                        session_id,
                        extra={
                            "event": "kg.consolidation.reset_manual_flag",
                            "candidate_id": cand_id,
                            "target_node_id": target_node_id,
                            "session_id": session_id,
                        },
                    )

            # Spec 4007e4a3 (Ideação #3, BR4 + BR5): UPDATE path must
            # preserve nodes that a human curator has explicitly marked
            # as human_curated. With an explicit override, update the target in
            # place and reset authorship instead of minting a colliding node.
            if (
                (
                    op == ReconciliationOperation.UPDATE
                    or (
                        is_relational_projection
                        and op == ReconciliationOperation.SUPERSEDE
                    )
                )
                and hint
                and getattr(hint, "target_node_id", None)
            ):
                target_node_id = hint.target_node_id
                node_type_check = _enum_value(cand.node_type)
                # Spec eca49df9 (FR6/TR6): a target UPDATE is an in-place
                # semantic update. The NC-8 reuse branch below stays a MERGE;
                # an UPDATE must not be miscounted as MERGE or CREATE.
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
                    **_semantic_projection_attrs(cand),
                    "human_curated": False,
                    # An explicit UPDATE is a new assertion just like the
                    # NC-8 reuse path below. Keep its provenance anchor in
                    # sync with the audit row written by this session.
                    **_session_provenance_attrs(
                        cand,
                        session_content_hash,
                        session_artifact_id,
                        seed_attestation=False,
                    ),
                }
                orch.update_node(node_type_check, target_node_id, update_attrs)
                _bump_attestation(orch, node_type_check, target_node_id)
                candidate_to_graph_id[cand_id] = target_node_id
                candidate_to_node_type[cand_id] = node_type_check
                if node_type_check in _COGNITIVE_SOURCE_TYPES:
                    # Explicit UPDATE used to bypass the durable ledger
                    # entirely.  Snapshot the literal post-update node so
                    # immutable replay validation can reject a divergent
                    # reuse of the same generation instead of silently
                    # accepting stale source data.
                    generation = _node_generation(
                        graph_scope, node_type_check, target_node_id
                    )
                    _queue_cognitive_source_record(
                        node_id=target_node_id,
                        node_type=node_type_check,
                        generation=generation,
                        attrs=_read_cognitive_source_node_attrs(
                            graph_scope, node_type_check, target_node_id
                        ),
                    )
                logger.info(
                    "kg.consolidation.updated candidate=%s target=%s "
                    "type=%s session=%s",
                    cand_id,
                    target_node_id,
                    node_type_check,
                    session_id,
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
                # Spec MKG-A-S1 (FR1/FR3, D1): deterministic successor id.
                # The successor mints generation = superseded generation + 1
                # so it NEVER collides with the superseded node while staying
                # stable across re-execution (rebuild/replay).
                successor_generation = (
                    _node_generation(graph_scope, node_type, superseded_id) + 1
                )
                new_node_id = mint_node_id(
                    board_id,
                    node_type,
                    derive_natural_key(cand.source_artifact_ref, node_type, cand.title),
                    successor_generation,
                )
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
                    "generation": successor_generation,
                    **_semantic_projection_attrs(cand),
                    # Spec MKG-B-S1 (FR2/FR3): successor is a fresh assertion.
                    **_session_provenance_attrs(
                        cand, session_content_hash, session_artifact_id
                    ),
                    "embedding": embedding,
                }

                # The deterministic successor id is also the idempotency
                # key for an explicit SUPERSEDE replay.  The embedded
                # adapter auto-commits each statement, so a prior attempt
                # may already have materialized the successor (and perhaps
                # completed the trail) even when the relational ACK was
                # retried.  Reconcile that exact successor instead of
                # issuing CREATE for its primary key again.
                existing_successor = _lookup_existing_node_identity_by_id(
                    graph_scope, node_type, new_node_id
                )
                if existing_successor is not None:
                    existing_ref = str(
                        existing_successor.get("source_artifact_ref") or ""
                    ).strip()
                    candidate_ref = str(cand.source_artifact_ref or "").strip()
                    if (
                        existing_ref and candidate_ref and existing_ref != candidate_ref
                    ) or (existing_ref and not candidate_ref):
                        raise ValueError(
                            "deterministic_identity_conflict: explicit "
                            "SUPERSEDE successor is bound to a different "
                            "source_artifact_ref; "
                            f"node_id={new_node_id} node_type={node_type} "
                            f"existing_ref={existing_ref!r} "
                            f"candidate_ref={candidate_ref!r}"
                        )

                    successor_semantics = _node_semantic_fields(
                        graph_scope,
                        node_type,
                        new_node_id,
                    )
                    successor_conflicts = (
                        decision_semantics_differ(
                            cand,
                            successor_semantics or {},
                        )
                        if node_type == "Decision"
                        else normalize_text(
                            _node_title(graph_scope, node_type, new_node_id)
                        )
                        != normalize_text(cand.title)
                    )
                    if successor_conflicts:
                        raise ValueError(
                            "deterministic_identity_conflict: explicit "
                            "SUPERSEDE successor has different assertion "
                            "semantics; "
                            f"node_id={new_node_id} node_type={node_type}"
                        )

                    linked_successor = _node_superseded_by(
                        graph_scope, node_type, superseded_id
                    )
                    if linked_successor not in (None, new_node_id):
                        raise ValueError(
                            "deterministic_supersede_conflict: target is "
                            "already linked to a different successor; "
                            f"target_node_id={superseded_id} "
                            f"existing_successor={linked_successor} "
                            f"candidate_successor={new_node_id}"
                        )

                    is_curated = _node_is_human_curated(
                        graph_scope, node_type, new_node_id
                    )
                    if not is_curated:
                        _apply_graph_node_update_partial(
                            orch, node_type, new_node_id, new_attrs
                        )
                    if candidate_ref and not existing_ref:
                        orch.update_node(
                            node_type,
                            new_node_id,
                            {"source_artifact_ref": candidate_ref},
                            count_candidate=False,
                        )
                    if linked_successor is None:
                        orch.mark_superseded(
                            node_type,
                            superseded_id,
                            superseded_by=new_node_id,
                            superseded_at=_now_iso(),
                            revocation_reason=("superseded by consolidation session"),
                        )
                    orch.create_edge(
                        "supersedes",
                        new_node_id,
                        superseded_id,
                        attrs={"confidence": 1.0},
                        from_type=node_type,
                        to_type=node_type,
                    )
                    _bump_attestation(orch, node_type, new_node_id)
                    candidate_to_graph_id[cand_id] = new_node_id
                    candidate_to_node_type[cand_id] = node_type
                    orch.counters.nodes_merged += 1
                    orch.counters.merge_audit_items.append(
                        {
                            "candidate_id": cand_id,
                            "node_type": node_type,
                            "source_artifact_ref": candidate_ref,
                            "reused_node_id": new_node_id,
                            "operation": "MERGE_SUPERSEDE_BY_DETERMINISTIC_ID",
                        }
                    )
                    # A graph-ahead crash can leave the deterministic
                    # successor materialized while its durable MKG-A source
                    # record is absent. Queue the same idempotent append as
                    # the fresh SUPERSEDE path before acknowledging replay.
                    _queue_cognitive_source_record(
                        node_id=new_node_id,
                        node_type=node_type,
                        generation=successor_generation,
                        attrs=_read_cognitive_source_node_attrs(
                            graph_scope, node_type, new_node_id
                        ),
                    )
                    logger.info(
                        "kg.consolidation.supersede_replayed candidate=%s "
                        "existing=%s target=%s type=%s session=%s",
                        cand_id,
                        new_node_id,
                        superseded_id,
                        node_type,
                        session_id,
                        extra={
                            "event": "kg.consolidation.supersede_replayed",
                            "candidate_id": cand_id,
                            "existing_id": new_node_id,
                            "superseded_node_id": superseded_id,
                            "node_type": node_type,
                            "session_id": session_id,
                        },
                    )
                    continue

                orch.supersede_node(
                    node_type,
                    new_node_id,
                    superseded_id,
                    new_attrs,
                    revocation_reason="superseded by consolidation session",
                )
                candidate_to_graph_id[cand_id] = new_node_id
                candidate_to_node_type[cand_id] = node_type
                _queue_cognitive_source_record(
                    node_id=new_node_id,
                    node_type=node_type,
                    generation=successor_generation,
                    attrs=new_attrs,
                )
                logger.info(
                    "kg.consolidation.superseded candidate=%s new=%s old=%s "
                    "type=%s session=%s",
                    cand_id,
                    new_node_id,
                    superseded_id,
                    node_type,
                    session_id,
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
            # already has a graph backend node from a prior session. If yes, reuse
            # it (UPDATE attrs unless human_curated). Without this branch
            # every spec.semantic_changed / spec.moved / spec.version_bumped
            # event spawns a duplicate Entity for the same source.
            source_ref = cand.source_artifact_ref or ""
            if source_ref:
                existing_id = _lookup_existing_node(graph_scope, node_type, source_ref)
                if existing_id:
                    is_curated = _node_is_human_curated(
                        graph_scope, node_type, existing_id
                    )
                    existing_semantics = _node_semantic_fields(
                        graph_scope,
                        node_type,
                        existing_id,
                    )
                    decision_semantic_change = (
                        node_type == "Decision"
                        and decision_semantics_differ(
                            cand,
                            existing_semantics or {},
                        )
                    )
                    # Spec MKG-D-S1 (FR8/D7): an identity-bearing change
                    # (normalized TITLE differs, or any semantic field for a
                    # Decision) on a non-curated reuse is
                    # a supersede-with-trail — the previous state is
                    # preserved as a walkable chain entry instead of
                    # evaporating under a destructive UPDATE.
                    # SK-A relational projections are a current-state cache of
                    # the immutable relational RDL.  Their source reference is
                    # the stable projection identity, so a changed Decision or
                    # Alternative must update that graph node in place.  The
                    # generic cognitive supersedence trail would duplicate RDL
                    # history in the KG and leave two owner edges for one
                    # source ref, making the exact active-set ambiguous.
                    if (
                        not is_curated
                        and not is_relational_projection
                        and (
                            decision_semantic_change
                            or normalize_text(cand.title)
                            != normalize_text(
                                _node_title(graph_scope, node_type, existing_id)
                            )
                        )
                    ):
                        trail_generation = (
                            _node_generation(graph_scope, node_type, existing_id) + 1
                        )
                        trail_node_id = mint_node_id(
                            board_id,
                            node_type,
                            derive_natural_key(
                                cand.source_artifact_ref, node_type, cand.title
                            ),
                            trail_generation,
                        )
                        embedding = embedder.encode(
                            f"{cand.title}\n{cand.content or ''}"
                        )
                        trail_attrs = {
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
                            "generation": trail_generation,
                            **_semantic_projection_attrs(cand),
                            # Spec MKG-B-S1 (FR2/FR3): trail successor is a
                            # fresh assertion — count restarts at 1.
                            **_session_provenance_attrs(
                                cand, session_content_hash, session_artifact_id
                            ),
                            "embedding": embedding,
                        }
                        orch.supersede_node(
                            node_type,
                            trail_node_id,
                            existing_id,
                            trail_attrs,
                            revocation_reason=(
                                "semantic change on NC-8 reuse (MKG-D trail)"
                            ),
                        )
                        candidate_to_graph_id[cand_id] = trail_node_id
                        candidate_to_node_type[cand_id] = node_type
                        _queue_cognitive_source_record(
                            node_id=trail_node_id,
                            node_type=node_type,
                            generation=trail_generation,
                            attrs=trail_attrs,
                        )
                        logger.info(
                            "kg.consolidation.reuse_superseded candidate=%s "
                            "old=%s new=%s type=%s session=%s",
                            cand_id,
                            existing_id,
                            trail_node_id,
                            node_type,
                            session_id,
                            extra={
                                "event": "kg.consolidation.reuse_superseded",
                                "candidate_id": cand_id,
                                "superseded_node_id": existing_id,
                                "new_node_id": trail_node_id,
                                "node_type": node_type,
                                "session_id": session_id,
                            },
                        )
                        continue
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
                            **_semantic_projection_attrs(cand),
                            # Spec MKG-B-S1 (FR3/D5): the rewrite is a NEW
                            # assertion — restamp the provenance anchor so
                            # drift clears after the re-consolidation remedy.
                            **_session_provenance_attrs(
                                cand,
                                session_content_hash,
                                session_artifact_id,
                                seed_attestation=False,
                            ),
                            "embedding": embedding,
                        }
                        _apply_graph_node_update_partial(
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
                        _apply_graph_node_update_partial(
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
                    # Spec MKG-B-S1 (FR4, D3): the reuse itself is a
                    # re-attestation — counted on BOTH branches (curated
                    # included: content is protected, maturity metadata
                    # is not).
                    _bump_attestation(orch, node_type, existing_id)
                    candidate_to_graph_id[cand_id] = existing_id
                    candidate_to_node_type[cand_id] = node_type
                    # Spec eca49df9 (FR5/AC6): count + audit the NC-8
                    # dedup-reuse (merge). No GraphWriteRecord — the reused
                    # node belongs to a prior session and must not enter
                    # compensation rollback.
                    orch.counters.nodes_merged += 1
                    orch.counters.merge_audit_items.append(
                        {
                            "candidate_id": cand_id,
                            "node_type": node_type,
                            "source_artifact_ref": source_ref,
                            "reused_node_id": existing_id,
                            "operation": "MERGE",
                        }
                    )
                    if node_type in _COGNITIVE_SOURCE_TYPES:
                        # Recovery for a graph-ahead NC-8 write: a prior
                        # auto-committed graph update may exist without the
                        # relational durable-source append. Snapshot the
                        # actual post-update node so rebuild remains literal.
                        reuse_generation = _node_generation(
                            graph_scope, node_type, existing_id
                        )
                        reuse_attrs = _read_cognitive_source_node_attrs(
                            graph_scope, node_type, existing_id
                        )
                        _queue_cognitive_source_record(
                            node_id=existing_id,
                            node_type=node_type,
                            generation=reuse_generation,
                            attrs=reuse_attrs,
                        )
                    logger.info(
                        "kg.consolidation.dedup_reused candidate=%s "
                        "existing=%s type=%s ref=%s session=%s curated=%s",
                        cand_id,
                        existing_id,
                        node_type,
                        source_ref,
                        session_id,
                        is_curated,
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

            # Spec MKG-A-S1 (FR1/FR2, D1/D2): deterministic id for fresh
            # CREATEs — same (board, type, natural key) always mints the
            # same id, so references survive graph rebuilds.
            node_id = mint_node_id(
                board_id,
                node_type,
                derive_natural_key(cand.source_artifact_ref, node_type, cand.title),
                0,
            )
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
                "generation": 0,
                **_semantic_projection_attrs(cand),
                # Spec MKG-B-S1 (FR2/FR3): extraction provenance + first
                # attestation recorded at birth.
                **_session_provenance_attrs(
                    cand, session_content_hash, session_artifact_id
                ),
                "embedding": embedding,
            }

            # A deterministic id is an idempotency key in its own right.
            # Normally NC-8 finds the node above by source_artifact_ref, but
            # an at-least-once replay can observe a node that was already
            # materialized while the source-ref lookup is unavailable or the
            # historical row has an empty source ref.  Retrying CREATE in
            # that state turns a successful prior materialization into a
            # permanent PRIMARY KEY DLQ.  Reuse the exact deterministic id
            # instead, while refusing to alias two non-empty source refs (a
            # genuine identity conflict must stay visible).
            existing_identity = _lookup_existing_node_identity_by_id(
                graph_scope, node_type, node_id
            )
            if existing_identity is not None:
                existing_ref = str(
                    existing_identity.get("source_artifact_ref") or ""
                ).strip()
                candidate_ref = str(cand.source_artifact_ref or "").strip()
                if (
                    existing_ref and candidate_ref and existing_ref != candidate_ref
                ) or (existing_ref and not candidate_ref):
                    raise ValueError(
                        "deterministic_identity_conflict: deterministic node id "
                        "is already bound to a different source_artifact_ref; "
                        f"node_id={node_id} node_type={node_type} "
                        f"existing_ref={existing_ref!r} "
                        f"candidate_ref={candidate_ref!r}"
                    )

                if node_type == "Decision":
                    existing_semantics = _node_semantic_fields(
                        graph_scope,
                        node_type,
                        node_id,
                    )
                    if existing_semantics is None or decision_semantics_differ(
                        cand, existing_semantics
                    ):
                        raise ValueError(
                            "deterministic_identity_conflict: refusing to "
                            "overwrite Decision semantics at an existing "
                            "generation; reconcile against the active lineage "
                            "with SUPERSEDE"
                        )

                is_curated = _node_is_human_curated(graph_scope, node_type, node_id)
                if not is_curated:
                    _apply_graph_node_update_partial(
                        orch, node_type, node_id, node_attrs
                    )
                # Empty source refs exist in historical/partially-written
                # rows.  Binding the ref is safe here because node_id was
                # derived from this exact candidate identity and non-empty
                # conflicting refs were rejected above.
                if candidate_ref and not existing_ref:
                    orch.update_node(
                        node_type,
                        node_id,
                        {"source_artifact_ref": candidate_ref},
                        count_candidate=False,
                    )
                _bump_attestation(orch, node_type, node_id)
                candidate_to_graph_id[cand_id] = node_id
                candidate_to_node_type[cand_id] = node_type
                orch.counters.nodes_merged += 1
                orch.counters.merge_audit_items.append(
                    {
                        "candidate_id": cand_id,
                        "node_type": node_type,
                        "source_artifact_ref": candidate_ref,
                        "reused_node_id": node_id,
                        "operation": "MERGE_BY_DETERMINISTIC_ID",
                    }
                )
                # Same graph-ahead recovery as explicit SUPERSEDE, for a
                # generation-zero CREATE whose deterministic id already
                # exists. The durable store deduplicates a surviving append.
                _queue_cognitive_source_record(
                    node_id=node_id,
                    node_type=node_type,
                    generation=0,
                    attrs=_read_cognitive_source_node_attrs(
                        graph_scope, node_type, node_id
                    ),
                )
                logger.info(
                    "kg.consolidation.deterministic_id_reused candidate=%s "
                    "existing=%s type=%s ref=%s session=%s curated=%s",
                    cand_id,
                    node_id,
                    node_type,
                    candidate_ref,
                    session_id,
                    is_curated,
                    extra={
                        "event": "kg.consolidation.deterministic_id_reused",
                        "candidate_id": cand_id,
                        "existing_id": node_id,
                        "node_type": node_type,
                        "source_artifact_ref": candidate_ref,
                        "session_id": session_id,
                        "was_curated_preserved": is_curated,
                    },
                )
                continue

            _apply_graph_node_create(orch, node_type, node_id, node_attrs)
            candidate_to_graph_id[cand_id] = node_id
            candidate_to_node_type[cand_id] = node_type
            _queue_cognitive_source_record(
                node_id=node_id,
                node_type=node_type,
                generation=0,
                attrs=node_attrs,
            )

        if clear_source_candidate_id is not None:
            source_graph_id = candidate_to_graph_id.get(clear_source_candidate_id)
            if source_graph_id is None:
                raise SpecLineageReconciliationError(
                    "spec_lineage_clear_source_unresolved",
                    "The canonical Spec Entity could not be resolved before "
                    "the explicit parent clear.",
                )
            orch.clear_spec_lineage_parent(source_graph_id)

        for edge in edge_candidates.values():
            from_id, from_xref_type = _resolve_endpoint(
                edge.from_candidate_id,
                candidate_to_graph_id,
                graph_scope=graph_scope,
            )
            to_id, to_xref_type = _resolve_endpoint(
                edge.to_candidate_id,
                candidate_to_graph_id,
                graph_scope=graph_scope,
            )
            if from_id is None or to_id is None:
                if str(getattr(edge, "rule_id", "") or "").startswith(
                    (
                        "supports/code_traceability_",
                        "derives_from/code_traceability_",
                        "belongs_to/code_traceability_",
                        "overlaps/code_traceability_",
                        "supersedes/code_traceability_",
                    )
                ):
                    raise KGPrimitiveError(
                        "code_traceability_kg_endpoint_unresolved",
                        "A persisted Code Traceability relation endpoint is "
                        "not materialized yet; retry the relational projection.",
                        session_id=session_id,
                        details={"edge_candidate_id": edge.candidate_id},
                    )
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
                _enum_value(from_cand.node_type)
                if from_cand
                else from_xref_type
                or candidate_to_node_type.get(edge.from_candidate_id)
            )
            to_hint = (
                _enum_value(to_cand.node_type)
                if to_cand
                else to_xref_type or candidate_to_node_type.get(edge.to_candidate_id)
            )
            orch.create_edge(
                edge_type=_enum_value(edge.edge_type),
                from_id=from_id,
                to_id=to_id,
                attrs=edge_attrs,
                from_type=from_hint,
                to_type=to_hint,
            )

        if relational_projection_active_set_intent is not None:
            from okto_pulse.core.kg.interfaces.graph_transaction import (
                ProjectionActiveSetIntent,
                ProjectionEdgeRef,
                ProjectionNodeRef,
            )

            active_nodes: list[ProjectionNodeRef] = []
            active_refs = tuple(
                getattr(
                    relational_projection_active_set_intent,
                    "active_refs",
                    (),
                )
            )
            active_candidate_ids = frozenset(
                str(getattr(ref, "candidate_id", "")) for ref in active_refs
            )
            if active_candidate_ids != frozenset(relational_projection_candidate_ids):
                raise KGPrimitiveError(
                    "relational_projection_active_set_mismatch",
                    "Projection ownership changed after session admission.",
                    session_id=session_id,
                )
            for ref in active_refs:
                candidate_id = str(getattr(ref, "candidate_id", ""))
                node_id = candidate_to_graph_id.get(candidate_id)
                if node_id is None:
                    raise KGPrimitiveError(
                        "relational_projection_member_unresolved",
                        "An active relational projection candidate could not "
                        "be resolved to a graph identity.",
                        session_id=session_id,
                        details={"candidate_id": candidate_id},
                    )
                active_nodes.append(
                    ProjectionNodeRef(
                        node_type=str(getattr(ref, "node_type", "")),
                        node_id=node_id,
                        source_artifact_ref=str(
                            getattr(ref, "source_artifact_ref", "")
                        ),
                    )
                )
            active_edges: list[ProjectionEdgeRef] = []
            active_edge_refs = tuple(
                getattr(
                    relational_projection_active_set_intent,
                    "active_edges",
                    (),
                )
            )
            declared_edge_candidate_ids = {
                str(getattr(ref, "candidate_id", "")) for ref in active_edge_refs
            }
            emitted_projection_edge_ids = {
                candidate_id
                for candidate_id, candidate in edge_candidates.items()
                if str(getattr(candidate, "rule_id", "") or "").startswith(
                    "precedes/spec_dependency/"
                )
            }
            if (
                "" in declared_edge_candidate_ids
                or len(declared_edge_candidate_ids) != len(active_edge_refs)
                or declared_edge_candidate_ids != emitted_projection_edge_ids
            ):
                raise KGPrimitiveError(
                    "relational_projection_active_set_mismatch",
                    "Projection edge candidates must exactly match the active set.",
                    session_id=session_id,
                )
            for ref in active_edge_refs:
                edge_candidate_id = str(getattr(ref, "candidate_id", ""))
                candidate = edge_candidates.get(edge_candidate_id)
                if candidate is None or (
                    _enum_value(candidate.edge_type)
                    != str(getattr(ref, "edge_type", ""))
                    or candidate.from_candidate_id
                    != str(getattr(ref, "from_candidate_id", ""))
                    or candidate.to_candidate_id
                    != str(getattr(ref, "to_candidate_id", ""))
                    or str(candidate.rule_id or "") != str(getattr(ref, "rule_id", ""))
                ):
                    raise KGPrimitiveError(
                        "relational_projection_edge_identity_mismatch",
                        "An active relational edge changed after session admission.",
                        session_id=session_id,
                    )
                from_id, from_type = _resolve_endpoint(
                    candidate.from_candidate_id,
                    candidate_to_graph_id,
                    graph_scope=graph_scope,
                )
                to_id, to_type = _resolve_endpoint(
                    candidate.to_candidate_id,
                    candidate_to_graph_id,
                    graph_scope=graph_scope,
                )
                from_type = from_type or candidate_to_node_type.get(
                    candidate.from_candidate_id
                )
                to_type = to_type or candidate_to_node_type.get(
                    candidate.to_candidate_id
                )
                if not from_id or not to_id or not from_type or not to_type:
                    raise KGPrimitiveError(
                        "relational_projection_edge_unresolved",
                        "An active relational projection edge has an unresolved endpoint.",
                        session_id=session_id,
                        details={"edge_candidate_id": edge_candidate_id},
                    )
                active_edges.append(
                    ProjectionEdgeRef(
                        edge_type=_enum_value(candidate.edge_type),
                        from_type=from_type,
                        to_type=to_type,
                        from_id=from_id,
                        to_id=to_id,
                        rule_id=str(candidate.rule_id or ""),
                    )
                )
            projection_owner_type = str(
                getattr(
                    relational_projection_active_set_intent,
                    "owner_type",
                    "",
                )
            )
            projection_owner_id = str(
                getattr(
                    relational_projection_active_set_intent,
                    "owner_id",
                    "",
                )
            )
            owner_source_ref = f"{projection_owner_type}:{projection_owner_id}"
            owner_candidate_ids = [
                candidate_id
                for candidate_id, candidate in node_candidates.items()
                if _enum_value(candidate.node_type) == "Entity"
                and str(candidate.source_artifact_ref or "") == owner_source_ref
            ]
            owner_node_id = None
            if owner_candidate_ids:
                if len(owner_candidate_ids) != 1:
                    raise KGPrimitiveError(
                        "relational_projection_owner_ambiguous",
                        "The projection owner resolves to multiple deterministic "
                        "root candidates.",
                        session_id=session_id,
                    )
                owner_node_id = candidate_to_graph_id.get(owner_candidate_ids[0])
                if owner_node_id is None:
                    raise KGPrimitiveError(
                        "relational_projection_owner_unresolved",
                        "The projection owner root could not be resolved to a "
                        "graph identity.",
                        session_id=session_id,
                    )
            orch.reconcile_projection_active_set(
                ProjectionActiveSetIntent(
                    owner_type=projection_owner_type,
                    owner_id=projection_owner_id,
                    namespace=str(
                        getattr(
                            relational_projection_active_set_intent,
                            "namespace",
                            "",
                        )
                    ),
                    owner_node_id=owner_node_id,
                    active_nodes=tuple(active_nodes),
                    active_edges=tuple(active_edges),
                )
            )

        # v0.3.0 R2: recompute relevance_score for every node touched by
        # this session. This includes nodes-only fallback commits, which
        # otherwise stay pinned to the neutral 0.5 score and inflate
        # kg_health.default_score_ratio.
        try:
            from okto_pulse.core.kg.scoring import _recompute_relevance_batch

            endpoints_to_recompute: list[tuple[str, str]] = []
            seen: set[tuple[str, str]] = set()
            for cand_id, node_id in candidate_to_graph_id.items():
                node_type = candidate_to_node_type.get(cand_id)
                if not node_type:
                    continue
                key = (node_type, node_id)
                if key not in seen:
                    seen.add(key)
                    endpoints_to_recompute.append(key)
            for edge in edge_candidates.values():
                from_id_resolved, from_type_resolved = _resolve_endpoint(
                    edge.from_candidate_id,
                    candidate_to_graph_id,
                    graph_scope=graph_scope,
                )
                if from_type_resolved is None:
                    from_type_resolved = candidate_to_node_type.get(
                        edge.from_candidate_id
                    )
                to_id_resolved, to_type_resolved = _resolve_endpoint(
                    edge.to_candidate_id,
                    candidate_to_graph_id,
                    graph_scope=graph_scope,
                )
                if to_type_resolved is None:
                    to_type_resolved = candidate_to_node_type.get(edge.to_candidate_id)
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
                for node_type, node_id in endpoints_to_recompute:
                    orch.protect_node_properties(
                        node_type,
                        node_id,
                        (
                            "last_recomputed_at",
                            "pre_cancellation_relevance_score",
                            "relevance_score",
                        ),
                    )
                _recompute_relevance_batch(
                    graph_scope,
                    board_id,
                    endpoints_to_recompute,
                    trigger="degree_delta",
                )
        except Exception as exc:
            logger.warning(
                "kg.scoring.commit_hook_failed session=%s err=%s",
                session_id,
                exc,
            )

        committed_at = datetime.now(timezone.utc)
        run_async_blocking(graph_scope.commit())
        return (
            candidate_to_graph_id,
            orch.counters,
            list(orch.records),
            committed_at,
            connectivity,
            cognitive_source_records,
        )

    except KGPrimitiveError as primitive_error:
        rollback_error: Exception | None = None
        try:
            run_async_blocking(graph_scope.rollback())
        except Exception as exc:
            rollback_error = exc
        if orch.records:
            try:
                _compensate_graph_writes(board_id, session_id, orch.records)
            except Exception as compensation_error:
                raise KGPrimitiveError(
                    "graph_compensation_failed",
                    "Graph mutation was rejected and its complete before-image "
                    "could not be restored; the session remains retryable and "
                    "must not be acknowledged.",
                    session_id=session_id,
                    details={
                        "original_error_code": primitive_error.code,
                        "rollback_failure_type": (
                            type(rollback_error).__name__
                            if rollback_error is not None
                            else None
                        ),
                        "compensation_failure_type": type(compensation_error).__name__,
                    },
                    retryable=True,
                ) from compensation_error
        if rollback_error is not None:
            raise KGPrimitiveError(
                "graph_scope_cleanup_failed",
                "Graph mutation was compensated, but the original writer "
                "scope could not be proven closed.",
                session_id=session_id,
                details={
                    "original_error_code": primitive_error.code,
                    "original_error_retryable": primitive_error.retryable,
                    "rollback_failure_type": type(rollback_error).__name__,
                },
                retryable=True,
            ) from rollback_error
        raise
    except Exception as exc:
        message, failure_details = _contextualize_graph_commit_error(exc)
        failure_code, failure_retryable = _graph_commit_failure_code(exc)
        rollback_error: Exception | None = None
        try:
            run_async_blocking(graph_scope.rollback())
        except Exception as cleanup_error:
            rollback_error = cleanup_error
        if orch.records:
            try:
                _compensate_graph_writes(board_id, session_id, orch.records)
            except Exception as compensation_error:
                raise KGPrimitiveError(
                    "graph_compensation_failed",
                    "Graph commit failed and its complete before-image could not "
                    "be restored; the session remains retryable and must not be "
                    "acknowledged.",
                    session_id=session_id,
                    details={
                        "original_error_code": failure_code,
                        "original_error_retryable": failure_retryable,
                        "original_error_context": failure_details,
                        "rollback_failure_type": (
                            type(rollback_error).__name__
                            if rollback_error is not None
                            else None
                        ),
                        "compensation_failure_type": type(compensation_error).__name__,
                    },
                    retryable=True,
                ) from compensation_error
        if rollback_error is not None:
            raise KGPrimitiveError(
                "graph_scope_cleanup_failed",
                "Graph commit was compensated, but the original writer scope "
                "could not be proven closed.",
                session_id=session_id,
                details={
                    "original_error_code": failure_code,
                    "original_error_retryable": failure_retryable,
                    "original_error_context": failure_details,
                    "rollback_failure_type": type(rollback_error).__name__,
                },
                retryable=True,
            ) from rollback_error
        if isinstance(exc, SpecLineageReconciliationError) and exc.preserve_progress:
            disposition = (
                "graph reconciliation requires retry; bounded progress was preserved"
                if exc.retryable
                else "graph reconciliation halted; bounded state was preserved "
                "for operator recovery"
            )
            failure_message = f"{disposition}: {message}"
        else:
            failure_message = f"commit failed and was compensated: {message}"
        raise KGPrimitiveError(
            failure_code,
            failure_message,
            session_id=session_id,
            details=failure_details,
            retryable=failure_retryable,
        ) from exc


async def commit_consolidation(
    req: CommitConsolidationRequest,
    *,
    agent_id: str,
    db=None,
    blocking_execution: BlockingExecutionPort | None = None,
    defer_session_finalization: bool = False,
) -> CommitConsolidationResponse:
    """Atomically write graph backend nodes/edges + audit + outbox event.

    Graph writes are offloaded to the thread pool via ``_run_graph_io`` and
    ``_do_graph_commit``.  The graph scope is closed before the atomic
    cognitive-source batch is appended on the owning async loop; no relational
    operation runs while the process-global embedded graph writer is held.
    Audit persistence (SQLite) remains in the async context via
    ``_commit_audit_records``.

    KG-01 FR5/FR6 enforcement: this primitive is a write path against
    `board graph` and MUST run inside a `under_safe_write` guard. In SOFT
    mode (default) a missing guard logs `kg.write_barrier.unguarded` and
    bumps `kg_unguarded_write_total` without blocking — production wires
    callers to enter the guard via KGSafeWriteLifecycle. In STRICT mode
    (tests + dedicated production deployments) the absence of a guard
    raises ``WriteLifecycleViolation`` before the graph backend write begins.
    """
    from okto_pulse.core.kg.write_barrier import require_write_token

    registry = get_kg_registry()
    session = await _require_open_session(
        req.session_id,
        agent_id,
        allow_pending_commit=True,
    )

    _require_code_traceability_candidate_ownership(
        dict(session.node_candidates),
        agent_id=agent_id,
        session_id=req.session_id,
    )

    if defer_session_finalization and db is None:
        raise KGPrimitiveError(
            "relational_context_required",
            "deferred consolidation finalization requires a caller-owned "
            "relational UnitOfWork",
            session_id=req.session_id,
        )

    # FR5/FR6 barrier check. Uses session.board_id so a multi-board
    # process never blocks the wrong board. Raises in STRICT, logs+counter
    # in SOFT.
    require_write_token(session.board_id)

    # Spec MKG-E-S1 (FR4): subtype opt-in validation BEFORE any write.
    await _validate_subtype_declarations(dict(session.node_candidates))

    # A cognitive candidate can take an in-place UPDATE/NC-8 path whose graph
    # mutation has no before-image for compensation. Resolve the durable
    # source port before opening the graph writer so a missing adapter leaves
    # no graph-ahead residue. Runtime database failures still use the bounded
    # saga below after the graph scope has closed.
    cognitive_source_store = None
    if any(
        _enum_value(candidate.node_type) in _COGNITIVE_SOURCE_TYPES
        and candidate_id not in session.relational_projection_candidate_ids
        for candidate_id, candidate in session.node_candidates.items()
    ):
        from okto_pulse.core.ports.kg_cognitive_source import (
            CognitiveSourceError,
            require_cognitive_source_store,
        )

        try:
            cognitive_source_store = require_cognitive_source_store()
            if db is not None and not callable(
                getattr(cognitive_source_store, "append_many_in_context", None)
            ):
                raise CognitiveSourceError(
                    "cognitive_source_context_append_unsupported",
                    board_id=session.board_id,
                    remediation=(
                        "Install a CognitiveSourceStore adapter that can stage "
                        "append_many_in_context in the caller-owned relational "
                        "unit of work."
                    ),
                )
        except CognitiveSourceError as exc:
            raise KGPrimitiveError(
                "kg_cognitive_source_unavailable",
                "cognitive durable-source adapter is unavailable; graph "
                "commit was rejected before its first write.",
                session_id=req.session_id,
                details={
                    "board_id": session.board_id,
                    "failure_reason": exc.failure_reason,
                    "node_id": exc.node_id,
                },
            ) from exc

    owns_deferred_claim = False

    async def _complete_commit(
        kg_health_state: str,
    ) -> CommitConsolidationResponse:
        nonlocal owns_deferred_claim
        request_payload = req.model_dump(mode="json")
        pending = getattr(session, "pending_commit", None)
        if pending is not None:
            if not isinstance(pending, _PendingConsolidationCommit):
                raise KGPrimitiveError(
                    "session_commit_state_invalid",
                    "Session contains an invalid deferred commit snapshot",
                    session_id=req.session_id,
                )
            # Re-check under ``session.lock``. Two callers may both pass the
            # optimistic adapter pre-check before the first one marks the
            # deferred snapshot in flight.
            if pending.in_flight:
                raise KGPrimitiveError(
                    "session_commit_in_progress",
                    f"Session {req.session_id} is awaiting relational commit "
                    "finalization",
                    session_id=req.session_id,
                )
            if not defer_session_finalization:
                raise KGPrimitiveError(
                    "session_commit_retry_requires_finalization",
                    "A graph-applied deferred commit must be retried through "
                    "a caller-owned relational UnitOfWork",
                    session_id=req.session_id,
                )
            if pending.request_payload != request_payload:
                raise KGPrimitiveError(
                    "session_commit_retry_mismatch",
                    "Deferred commit retry must use the original summary and "
                    "agent overrides",
                    session_id=req.session_id,
                )

            # The session lock serializes this transition. Record ownership
            # before the first fallible restage await so cancellation cleanup
            # can distinguish this retry from a competing caller rejected on
            # another invocation's ``in_flight`` snapshot.
            owns_deferred_claim = True

            # The graph was already applied by the first attempt.  Restage only
            # the relational ledger/audit/outbox in the fresh caller UOW.
            await _append_cognitive_source_records(
                session.board_id,
                req.session_id,
                list(pending.cognitive_source_records),
                context=db,
                store=cognitive_source_store,
            )
            await _commit_audit_records(
                registry,
                db,
                list(pending.records),
                pending.counters,
                req,
                session,
                agent_id,
                pending.response.committed_at,
            )
            pending.in_flight = True
            session.touch(registry.require_session_store().default_ttl_seconds)
            return pending.response

        effective_hints = dict(session.reconciliation_hints)
        for cid, override in req.agent_overrides.items():
            effective_hints[cid] = override

        # --- graph backend writes (offloaded to thread pool) ---
        try:
            (
                candidate_to_graph_id,
                counters,
                records,
                committed_at,
                connectivity,
                cognitive_source_records,
            ) = await _run_graph_io(
                _do_graph_commit,
                session.board_id,
                req.session_id,
                dict(session.node_candidates),
                dict(session.edge_candidates),
                effective_hints,
                agent_id,
                registry.require_embedding_provider(),
                kg_health_state,
                session.content_hash,
                session.artifact_id,
                frozenset(req.agent_overrides),
                session.artifact_type,
                session.spec_lineage_parent_intent,
                getattr(
                    session,
                    "relational_projection_candidate_ids",
                    frozenset(),
                ),
                getattr(
                    session,
                    "relational_projection_active_set_intent",
                    None,
                ),
                executor=blocking_execution,
            )
        except KGPrimitiveError:
            raise
        except Exception as exc:
            message, details = _contextualize_graph_commit_error(exc)
            raise KGPrimitiveError(
                "commit_failed",
                f"graph backend commit failed: {message}",
                session_id=req.session_id,
                details=details,
            ) from exc

        # --- relational staging (async, graph writer already released) ---
        # Ladybug/Kuzu can auto-commit individual graph statements, so this is
        # deliberately a saga rather than a cross-engine transaction.  Both
        # the durable cognitive append and audit/outbox staging belong to the
        # same post-graph compensation barrier: neither may fail while leaving
        # graph state acknowledged as committed. In-place UPDATE/NC-8,
        # attestation, supersedence and relational projection mutations all
        # carry exact before-images in GraphWriteRecord.
        failure_stage = "cognitive_source_append"
        try:
            await _append_cognitive_source_records(
                session.board_id,
                req.session_id,
                cognitive_source_records,
                context=db,
                store=cognitive_source_store,
            )

            failure_stage = "audit_outbox_stage"
            await _commit_audit_records(
                registry,
                db,
                records,
                counters,
                req,
                session,
                agent_id,
                committed_at,
            )
        except Exception as staging_error:
            try:
                await _run_graph_io(
                    _compensate_graph_writes,
                    session.board_id,
                    req.session_id,
                    records,
                    executor=blocking_execution,
                )
            except Exception as compensation_error:
                logger.error(
                    "kg.post_graph.compensation_failed board=%s session=%s "
                    "failure_stage=%s staging_error=%s compensation_error=%s",
                    session.board_id,
                    req.session_id,
                    failure_stage,
                    staging_error,
                    compensation_error,
                    extra={
                        "event": "kg.post_graph.compensation_failed",
                        "board_id": session.board_id,
                        "session_id": req.session_id,
                        "failure_stage": failure_stage,
                    },
                )
                raise KGPrimitiveError(
                    "graph_compensation_failed",
                    "Relational staging failed and the graph before-image could "
                    "not be restored; the session remains retryable and must "
                    "not be acknowledged.",
                    session_id=req.session_id,
                    details={
                        "failure_stage": failure_stage,
                        "staging_failure_type": type(staging_error).__name__,
                        "compensation_failure_type": type(compensation_error).__name__,
                    },
                ) from compensation_error
            raise

        response = CommitConsolidationResponse(
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

        if defer_session_finalization:
            session.pending_commit = _PendingConsolidationCommit(
                request_payload=request_payload,
                records=tuple(records),
                counters=counters,
                cognitive_source_records=tuple(cognitive_source_records),
                response=response,
                in_flight=True,
            )
            owns_deferred_claim = True
            session.touch(registry.require_session_store().default_ttl_seconds)
            return response

        await _finalize_consolidation_session_unlocked(
            registry,
            session,
            records,
            session_id=req.session_id,
        )
        return response

    # Waiting for the session lock and resolving pre-commit health remain
    # cancellable: no irreversible graph write has started at either point.
    # Once graph IO begins, keep the parent-held lock until graph, audit/outbox,
    # and session finalization have all completed.
    async with session.lock:
        _validate_session_state(
            session,
            allow_pending_commit=True,
        )
        _require_code_traceability_candidate_ownership(
            dict(session.node_candidates),
            agent_id=agent_id,
            session_id=req.session_id,
        )
        kg_health_state = await _resolve_commit_kg_health_state(
            session.board_id,
            db,
        )
        # A health-reader failure is normalized to ``recovery_needed``.
        # Reject it here, before dispatching the graph callback, so neither
        # a stale permissive cache entry nor the callback itself can open the
        # graph. A pending deferred commit is relational-only on retry and has
        # already passed this gate before its graph mutation.
        if getattr(session, "pending_commit", None) is None:
            _validate_degraded_connectivity_before_open(
                board_id=session.board_id,
                session_id=req.session_id,
                node_candidates=dict(session.node_candidates),
                edge_candidates=dict(session.edge_candidates),
                writer_path=_connectivity_writer_path(agent_id),
                kg_health_state=kg_health_state,
            )
        try:
            return await _run_cancellation_atomic(
                _complete_commit(kg_health_state),
                task_name="core.kg.commit_consolidation",
            )
        except asyncio.CancelledError:
            # The cancellation drain above lets the atomic child finish.  A
            # deferred child may therefore have applied graph writes and
            # staged its relational batch even though its caller never reaches
            # ``uow.commit()``. Compensate while this invocation still owns the
            # session lock. Merely releasing the volatile retry snapshot would
            # leave graph-ahead state if the cancelled client never returned.
            pending = getattr(session, "pending_commit", None)
            if (
                defer_session_finalization
                and owns_deferred_claim
                and isinstance(pending, _PendingConsolidationCommit)
            ):
                await _run_cancellation_atomic(
                    _abort_deferred_consolidation_unlocked(
                        registry,
                        session,
                        req.session_id,
                        blocking_execution=blocking_execution,
                    ),
                    task_name="core.kg.cancelled_deferred_compensation",
                )
            raise


async def _finalize_consolidation_session_unlocked(
    registry,
    session: ConsolidationSession,
    records,
    *,
    session_id: str,
) -> None:
    """Finalize process-local session state after relational durability."""

    session.status = SessionStatus.COMMITTED
    session.committed_graph_node_refs = [
        {
            "node_id": record.entity_id,
            "node_type": record.entity_type,
            "kind": record.kind,
        }
        for record in records
    ]
    session.pending_commit = None
    await registry.require_session_store().remove(session_id)

    registry.require_cache_backend().invalidate_board(session.board_id)

    # Only a confirmed relational commit proves the whole write path healthy.
    try:
        from okto_pulse.core.kg.memory_pressure_collector import (
            record_write_success,
        )

        record_write_success(session.board_id)
    except Exception:  # pragma: no cover - observability must not break commit
        pass
    _COMMIT_HEALTH_CACHE.pop(session.board_id, None)


async def finalize_deferred_consolidation(
    session_id: str,
    *,
    agent_id: str,
) -> None:
    """Remove a deferred session only after its caller UOW committed.

    This hook deliberately performs no relational work.  Its sole authority is
    the caller's successful ``commit()`` return; until then the pending snapshot
    remains retryable and graph writes are never replayed.
    """

    registry = get_kg_registry()
    session = await registry.require_session_store().get(session_id)
    if session is None:
        # Terminal finalization is intentionally idempotent.  A caller may be
        # retrying after the relational commit succeeded and the first
        # finalizer removed the process-local session before a later cache or
        # transport failure became visible.
        return
    if not session.check_ownership(agent_id):
        raise _ownership(session_id, agent_id)

    async with session.lock:
        pending = getattr(session, "pending_commit", None)
        if not isinstance(pending, _PendingConsolidationCommit):
            raise KGPrimitiveError(
                "session_commit_not_pending",
                f"Session {session_id} has no deferred commit to finalize",
                session_id=session_id,
            )
        # The successful caller-owned relational commit is the authority for
        # terminal cleanup.  Do not require ``in_flight`` here: an earlier
        # error handler from an older caller may have released that volatile
        # flag after durability was already established.  Finalization must
        # never restage INSERTs or replay graph writes.
        await _finalize_consolidation_session_unlocked(
            registry,
            session,
            pending.records,
            session_id=session_id,
        )


async def release_deferred_consolidation(
    session_id: str,
    *,
    agent_id: str,
) -> None:
    """Release an unsuccessful caller UOW while preserving retry state."""

    registry = get_kg_registry()
    session = await registry.require_session_store().get(session_id)
    if session is None:
        return
    if not session.check_ownership(agent_id):
        raise _ownership(session_id, agent_id)

    async with session.lock:
        pending = getattr(session, "pending_commit", None)
        if isinstance(pending, _PendingConsolidationCommit):
            pending.in_flight = False
            session.touch(registry.require_session_store().default_ttl_seconds)


async def _abort_deferred_consolidation_unlocked(
    registry,
    session: ConsolidationSession,
    session_id: str,
    *,
    blocking_execution: BlockingExecutionPort | None = None,
) -> None:
    """Compensate/discard a deferred session while its lock is already held."""

    pending = getattr(session, "pending_commit", None)
    if isinstance(pending, _PendingConsolidationCommit):
        pending.in_flight = False
        await _run_graph_io(
            _compensate_graph_writes,
            session.board_id,
            session_id,
            list(pending.records),
            executor=blocking_execution,
        )
    session.pending_commit = None
    session.status = SessionStatus.ABORTED
    await registry.require_session_store().remove(session_id)
    registry.require_cache_backend().invalidate_board(session.board_id)


async def abort_deferred_consolidation(
    session_id: str,
    *,
    agent_id: str,
    blocking_execution: BlockingExecutionPort | None = None,
) -> None:
    """Compensate a deferred graph write and discard its session.

    Cognitive closeout uses this after its relational ``commit()`` fails.  Its
    next worker attempt must not mistake a graph-only node for a durable
    success, so this path compensates before removing the process-local retry
    snapshot.
    """

    registry = get_kg_registry()
    session = await registry.require_session_store().get(session_id)
    if session is None:
        return
    if not session.check_ownership(agent_id):
        raise _ownership(session_id, agent_id)

    async with session.lock:
        await _abort_deferred_consolidation_unlocked(
            registry,
            session,
            session_id,
            blocking_execution=blocking_execution,
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
    graph_scope, node_type: str, source_artifact_ref: str
) -> str | None:
    """Lookup the active graph node by type and source artifact reference.

    Supersedence deliberately retains every historical generation with the
    same ``source_artifact_ref``.  A bare ``LIMIT 1`` can therefore select a
    superseded generation and make an at-least-once replay mint its already
    materialized successor again.  Select only the active node and break ties
    deterministically by highest generation, then id (legacy NULL generation
    is generation zero).  Returns the graph node id if found, ``None``
    otherwise.  Used when NOOP to find existing nodes so edges can still be
    resolved and by NC-8 to update/supersede the current assertion.
    """
    if not source_artifact_ref:
        return None
    cypher = (
        f"MATCH (n:{node_type}) "
        "WHERE n.source_artifact_ref = $ref "
        "AND n.superseded_by IS NULL "
        "RETURN n.id "
        "ORDER BY coalesce(n.generation, 0) DESC, n.id DESC "
        "LIMIT 1"
    )
    try:
        res = graph_scope.execute(cypher, {"ref": source_artifact_ref})
        if res.rows:
            return res.rows[0][0]
    except Exception:
        pass
    return None


def _lookup_existing_node_identity_by_id(
    graph_scope, node_type: str, node_id: str
) -> dict[str, object] | None:
    """Return the minimal identity record for an exact deterministic id.

    Unlike the legacy source-ref lookup, this is the replay safety net for
    at-least-once materialization.  Read failures remain best-effort so graph
    adapters that cannot execute the lookup preserve their existing error
    behavior at CREATE; production semantic transaction adapters materialize
    ``GraphStatementResult.rows``.
    """

    if not node_id:
        return None
    try:
        result = graph_scope.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id "
            "RETURN n.id, n.source_artifact_ref LIMIT 1",
            {"id": node_id},
        )
        rows = getattr(result, "rows", ())
        if rows:
            row = rows[0]
            return {
                "id": row[0],
                "source_artifact_ref": row[1],
            }
    except Exception:
        pass
    return None


def _node_is_human_curated(graph_scope, node_type: str, node_id: str) -> bool:
    """Check whether a graph backend node has the human_curated flag set.

    Treats NULL as FALSE — legacy nodes from before v0.3.2 have no value
    set and must default to agent-managed semantics for retrocompat.
    Returns False on any read error so the UPDATE path defaults to the
    legacy behaviour rather than silently swallowing edits.
    """
    if not node_id:
        return False
    cypher = f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.human_curated LIMIT 1"
    try:
        res = graph_scope.execute(cypher, {"id": node_id})
        if res.rows:
            value = res.rows[0][0]
            return bool(value) if value is not None else False
    except Exception:
        pass
    return False


# Spec MKG-A-S1: canonical cognitive node types with a durable source
# (kept in lockstep with canonical_cognitive_preservation.COGNITIVE_TYPES).
_COGNITIVE_SOURCE_TYPES: tuple[str, ...] = (
    "Decision",
    "Learning",
    "Alternative",
    "Assumption",
)


_SOURCE_QUOTE_MAX_CHARS = 500


def _provenance_attrs(
    cand, session_content_hash: str, *, seed_attestation: bool = True
) -> dict:
    """Spec MKG-B-S1 (FR1/FR2/FR3): extraction provenance + attestation seed
    written on every node materialization (CREATE / SUPERSEDE / NC-8 trail).

    Optional span/extraction fields come from the candidate; the quote is
    truncated at this boundary (BR2) so an oversized agent payload can never
    bloat the graph. ``attestation_count`` starts at 1 — a node has been
    asserted exactly once at birth.

    ``seed_attestation=False`` is the NC-8 in-place UPDATE variant: the
    content is being rewritten by a NEW assertion, so the provenance anchor
    (span/extraction/source_content_hash) is restamped to describe the new
    content — without the restamp, kg_provenance_drift would keep flagging
    the node forever after the re-consolidation remedy (D5). Attestation
    counters are excluded there — ``_bump_attestation`` accumulates them
    (N -> N+1, never reset to 1).
    """

    quote = getattr(cand, "source_span_quote", None)
    if quote is not None:
        quote = quote[:_SOURCE_QUOTE_MAX_CHARS]
    attrs = {
        "source_span_start": getattr(cand, "source_span_start", None),
        "source_span_end": getattr(cand, "source_span_end", None),
        "source_span_quote": quote,
        "extraction_model_id": getattr(cand, "extraction_model_id", None),
        "extraction_prompt_hash": getattr(cand, "extraction_prompt_hash", None),
        "source_content_hash": (
            getattr(cand, "source_content_hash", None) or session_content_hash or None
        ),
    }
    if seed_attestation:
        attrs["attestation_count"] = 1
        attrs["last_attested_at"] = _now_iso()
    return attrs


def _session_provenance_attrs(
    cand,
    session_content_hash: str,
    session_artifact_id: str,
    *,
    seed_attestation: bool = True,
) -> dict:
    """Stamp only provenance that the current artifact session can prove.

    Layer-1 sessions may carry reference candidates for parent artifacts. For
    example, a card consolidation can reconcile its sprint Entity. The card's
    content hash must never replace the sprint Entity's provenance anchor.
    External references still count as attestations, but keep any existing
    source span/hash untouched until their own artifact session runs.
    """

    source_ref = str(getattr(cand, "source_artifact_ref", "") or "")
    artifact_id = str(session_artifact_id or "").strip().lower()
    owns_source = bool(
        artifact_id
        and any(part.strip().lower() == artifact_id for part in source_ref.split(":"))
    )
    if owns_source:
        return _provenance_attrs(
            cand,
            session_content_hash,
            seed_attestation=seed_attestation,
        )
    if seed_attestation:
        return {
            "attestation_count": 1,
            "last_attested_at": _now_iso(),
        }
    return {}


def _bump_attestation(orch, node_type: str, node_id: str) -> None:
    """Spec MKG-B-S1 (FR4, D3): NC-8 reuse re-attests the node — the counter
    accumulates as a maturity signal even when the content write is skipped
    (human_curated). NULL-safe: a legacy node without the column value counts
    as one prior attestation.
    """

    orch.increment_attestation(
        node_type,
        node_id,
        attested_at=_now_iso(),
    )


async def _validate_subtype_declarations(node_candidates: dict) -> None:
    """Spec MKG-E-S1 (FR4/BR2, D3): opt-in fail-closed subtype validation.

    Candidates WITHOUT kind_of pass untouched (the entire pre-MKG-E flow is
    byte-compatible). Candidates WITH kind_of require the registry (an
    edition without the port fails closed) and every (node_type, kind_of)
    pair must be declared — otherwise the commit aborts BEFORE any graph
    write with the actionable code ``kg_subtype_undeclared``.
    """

    pairs: set = set()
    for cand in node_candidates.values():
        kind_of = getattr(cand, "kind_of", None)
        if kind_of:
            pairs.add((_enum_value(cand.node_type), kind_of))
    if not pairs:
        return

    from okto_pulse.core.kg.schema_contract import (
        CODE_TRACEABILITY_ENTITY_SUBTYPES,
    )
    from okto_pulse.core.ports.kg_subtype_registry import (
        SubtypeRegistryError,
        normalize_kind_of,
        require_node_subtype_registry,
    )

    system_declared_keys = {
        ("Entity", normalize_kind_of(kind_of))
        for kind_of in CODE_TRACEABILITY_ENTITY_SUBTYPES
    }
    unresolved_pairs = {
        (node_type, kind_of)
        for node_type, kind_of in pairs
        if (node_type, normalize_kind_of(kind_of)) not in system_declared_keys
    }
    if not unresolved_pairs:
        return

    try:
        registry = require_node_subtype_registry()
        declared = await registry.list_all()
    except SubtypeRegistryError as exc:
        raise KGPrimitiveError(
            "kg_subtype_registry_unavailable",
            str(exc),
            details={"remediation": exc.remediation},
        ) from exc

    declared_by_type: dict = {
        "Entity": list(CODE_TRACEABILITY_ENTITY_SUBTYPES),
    }
    declared_keys: set = set(system_declared_keys)
    for declaration in declared:
        declared_by_type.setdefault(declaration.node_type, []).append(
            declaration.kind_of
        )
        declared_keys.add(
            (declaration.node_type, normalize_kind_of(declaration.kind_of))
        )

    for node_type, kind_of in sorted(unresolved_pairs):
        if (node_type, normalize_kind_of(kind_of)) not in declared_keys:
            raise KGPrimitiveError(
                "kg_subtype_undeclared",
                (
                    f"kind_of {kind_of!r} is not declared for node_type "
                    f"{node_type} — declare it first (kg subtype declare)."
                ),
                details={
                    "node_type": node_type,
                    "kind_of": kind_of,
                    "declared_subtypes": sorted(declared_by_type.get(node_type, [])),
                },
            )


def _do_count_only_attestation(
    board_id: str,
    refs: tuple[tuple[str, str], ...],
) -> tuple[tuple[tuple[str, str], ...], tuple[dict[str, str], ...]]:
    """Apply each count-only bump and report every bounded failure.

    The embedded graph adapter auto-commits statements, so one failed node
    cannot make already-applied bumps disappear. Returning the successful
    refs lets the owning session retry only the remainder after durability,
    release, or per-node failures.
    """

    from okto_pulse.core.kg.write_barrier import require_write_token

    require_write_token(board_id)

    async def _run():
        bumped: list[tuple[str, str]] = []
        failures: list[dict[str, str]] = []
        try:
            scope = await get_kg_registry().graph_transaction.begin(board_id)
        except Exception as exc:
            return (), (
                {
                    "phase": "begin",
                    "failure_type": type(exc).__name__,
                },
            )

        for node_type, node_id in refs:
            try:
                actual_types = scope.find_node_types(node_id)
                if node_type not in actual_types:
                    raise LookupError("audited graph node is missing")
                scope.increment_attestation(
                    node_type,
                    node_id,
                    attested_at=_now_iso(),
                )
                bumped.append((node_type, node_id))
            except Exception as exc:
                failures.append(
                    {
                        "phase": "increment",
                        "node_type": node_type,
                        "node_id": node_id,
                        "failure_type": type(exc).__name__,
                    }
                )

        try:
            await scope.commit()
        except Exception as exc:
            failures.append(
                {
                    "phase": "commit",
                    "failure_type": type(exc).__name__,
                }
            )
            try:
                await scope.rollback()
            except Exception as rollback_exc:
                failures.append(
                    {
                        "phase": "rollback",
                        "failure_type": type(rollback_exc).__name__,
                    }
                )

        return tuple(bumped), tuple(failures)

    return run_async_blocking(_run())


async def _register_count_only_attestation(
    registry,
    *,
    session: ConsolidationSession,
    previous_session_id: str | None,
    agent_id: str,
    db,
) -> None:
    """Register one unchanged re-assertion under the full write boundary.

    Progress is retained only after a graph bump was actually applied. If the
    durability lifecycle or writer release fails, a retry reuses that progress
    and never increments the same node twice. The public attested flag is set
    only after all refs, durability, and lock release succeed.
    """

    if not previous_session_id:
        raise KGPrimitiveError(
            "count_only_attestation_source_missing",
            "nothing_changed audit is missing its origin session",
            session_id=session.session_id,
        )

    kg_health_state = await _resolve_commit_kg_health_state(session.board_id, db)
    _validate_degraded_connectivity_before_open(
        board_id=session.board_id,
        session_id=session.session_id,
        node_candidates=dict(session.node_candidates),
        edge_candidates=dict(session.edge_candidates),
        writer_path=_connectivity_writer_path(agent_id),
        kg_health_state=kg_health_state,
    )

    getter = getattr(registry.audit_repo, "get_node_refs_by_session", None)
    if getter is None:
        raise KGPrimitiveError(
            "count_only_attestation_source_unavailable",
            "audit repository cannot resolve origin-session node references",
            session_id=session.session_id,
        )
    try:
        audit_refs = await getter(previous_session_id)
    except Exception as exc:
        raise KGPrimitiveError(
            "count_only_attestation_source_unavailable",
            "origin-session node references could not be resolved",
            session_id=session.session_id,
            details={"failure_type": type(exc).__name__},
        ) from exc

    refs = tuple(
        sorted({(ref.graph_node_type, ref.graph_node_id) for ref in audit_refs})
    )
    if not refs:
        session.count_only_attested = True
        return

    progress_attr = "_count_only_attestation_progress"
    progress = getattr(session, progress_attr, None)
    if progress is not None:
        if (
            progress.get("previous_session_id") != previous_session_id
            or tuple(progress.get("refs", ())) != refs
        ):
            raise KGPrimitiveError(
                "count_only_attestation_state_invalid",
                "session count-only retry state does not match the origin audit",
                session_id=session.session_id,
            )
        completed = {tuple(ref) for ref in progress.get("completed", ())}
    else:
        completed = set()

    remaining = tuple(ref for ref in refs if ref not in completed)
    failures: tuple[dict[str, str], ...] = ()
    mutation_ref = f"{session.session_id}:count_only_attestation"
    cancelled_during_graph_write = False
    try:
        with guarded_board_write(
            session.board_id,
            operation="count_only_attestation",
            owner_id=agent_id,
            mutation_ref=mutation_ref,
        ) as lease:
            bumped: tuple[tuple[str, str], ...] = ()
            if remaining:
                graph_task = asyncio.create_task(
                    _run_graph_io(
                        _do_count_only_attestation,
                        session.board_id,
                        remaining,
                    ),
                    name="core.kg.count_only_attestation",
                )
                try:
                    bumped, failures = await asyncio.shield(graph_task)
                except asyncio.CancelledError:
                    # The embedded adapter may have auto-committed a bump.
                    # Drain and record its exact result before completing the
                    # durability boundary, then propagate cancellation after
                    # the session idempotency marker is safe.
                    cancelled_during_graph_write = True
                    while not graph_task.done():
                        try:
                            await asyncio.shield(graph_task)
                        except asyncio.CancelledError:
                            continue
                    bumped, failures = graph_task.result()
            if bumped:
                completed.update(bumped)
                setattr(
                    session,
                    progress_attr,
                    {
                        "previous_session_id": previous_session_id,
                        "refs": refs,
                        "completed": tuple(sorted(completed)),
                    },
                )

            await run_blocking_graph_io(
                lease.ensure_durable,
                task_name="core.kg.count_only_attestation_durability",
            )
            if failures:
                raise KGPrimitiveError(
                    "count_only_attestation_failed",
                    "one or more origin nodes could not be re-attested",
                    session_id=session.session_id,
                    details={"failures": list(failures)},
                )
            if completed != set(refs):
                raise KGPrimitiveError(
                    "count_only_attestation_failed",
                    "count-only attestation completed an incomplete node set",
                    session_id=session.session_id,
                    details={
                        "expected_nodes": len(refs),
                        "completed_nodes": len(completed),
                    },
                )
    except GuardedWriteError as exc:
        raise KGPrimitiveError(
            "count_only_attestation_failed",
            "count-only attestation write boundary failed",
            session_id=session.session_id,
            details={
                "failure_code": exc.code,
                "retryable": exc.retryable,
                **exc.details,
            },
        ) from exc

    session.count_only_attested = True
    if hasattr(session, progress_attr):
        delattr(session, progress_attr)
    logger.info(
        "kg.attestation.count_only board=%s artifact_id=%s "
        "origin_session=%s trigger=propose nodes=%d",
        session.board_id,
        session.artifact_id,
        previous_session_id,
        len(refs),
        extra={
            "event": "kg.attestation.count_only",
            "board_id": session.board_id,
            "artifact_id": session.artifact_id,
            "origin_session_id": previous_session_id,
            "trigger": "propose",
            "nodes_attested": [node_id for _node_type, node_id in refs],
        },
    )
    if cancelled_during_graph_write:
        raise asyncio.CancelledError


def _read_cognitive_source_node_attrs(
    graph_scope, node_type: str, node_id: str
) -> dict:
    """Read the literal rebuild payload for an existing cognitive node.

    NC-8 reuses an existing generation and deliberately updates only mutable
    fields.  Reconstructing its durable-source record from the candidate would
    therefore lose historical fields such as ``created_at`` and the immutable
    embedding.  Read every stable graph property after the update instead.
    """

    from okto_pulse.core.kg.schema_contract import STABLE_NODE_PROPERTIES

    property_names = tuple(name for name in STABLE_NODE_PROPERTIES if name != "id") + (
        "embedding",
    )
    return_clause = ", ".join(f"n.{name}" for name in property_names)
    result = graph_scope.execute(
        f"MATCH (n:{node_type}) WHERE n.id = $id RETURN {return_clause} LIMIT 1",
        {"id": node_id},
    )
    rows = getattr(result, "rows", ())
    if not rows:
        raise RuntimeError(
            "cognitive_source_snapshot_missing: graph node disappeared "
            f"before durable append; node_id={node_id} node_type={node_type}"
        )
    row = rows[0]
    if len(row) != len(property_names):
        raise RuntimeError(
            "cognitive_source_snapshot_invalid: graph adapter returned an "
            f"incomplete payload; node_id={node_id} node_type={node_type}"
        )
    return {
        name: _cognitive_source_json_value(value)
        for name, value in zip(property_names, row, strict=True)
    }


def _cognitive_source_json_value(value):
    """Normalize graph-native values for the relational JSON payload."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_cognitive_source_json_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _cognitive_source_json_value(item) for key, item in value.items()
        }
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _cognitive_source_json_value(tolist())
    return value


def _cognitive_source_record_kwargs(
    *,
    board_id: str,
    session_id: str,
    node_id: str,
    node_type: str,
    generation: int,
    attrs: dict,
) -> dict:
    """Build the durable-source record payload for one cognitive node.

    ``payload`` carries every attribute persisted on the graph node
    (embedding included) so a rebuild replay is a literal restoration
    (spec FR4/BR3). ``evidence_refs`` preserves the original binding.
    """

    payload = dict(attrs)
    source_ref = str(payload.get("source_artifact_ref") or "")
    source_revision = max(int(payload.get("attestation_count") or 1) - 1, 0)
    return {
        "node_id": node_id,
        "board_id": board_id,
        "node_type": node_type,
        "generation": generation,
        "source_revision": source_revision,
        "payload": payload,
        "evidence_refs": (source_ref,) if source_ref else (),
        "source_session_id": session_id,
        "committed_at": _now_iso(),
    }


async def _restore_sealed_birth_fields(
    board_id: str,
    session_id: str,
    records: list[dict],
    *,
    context: object,
    store: object,
) -> list[dict]:
    """Re-seat re-derived birth stamps on what the durable ledger already sealed.

    The graph is a projection and may fall behind the ledger (restore from an
    older copy, targeted removal, rebuild, DLQ replay). Consolidation then
    materializes a node it believes to be new and stamps a fresh ``created_at``,
    which re-presents an immutable revision with divergent content and poisons
    the queue fail-closed (observed live on decision_059d5828). BR2 makes the
    relational ledger the authority, so its sealed birth wins.

    Fail-closed: a store that cannot answer aborts the commit. Degrading to the
    re-derived stamp would reintroduce exactly the corruption this prevents.
    """

    from okto_pulse.core.ports.kg_cognitive_source import (
        CognitiveSourceError,
        cognitive_source_semantic_key,
        restore_sealed_birth_fields,
    )

    lookup = getattr(store, "sealed_birth_payloads_in_context", None)
    if not callable(lookup):
        raise CognitiveSourceError(
            "cognitive_source_birth_lookup_unsupported",
            board_id=board_id,
            remediation=(
                "Install a CognitiveSourceStore adapter exposing "
                "sealed_birth_payloads_in_context(); the durable ledger is the "
                "authority for a cognitive node's birth stamp and consolidation "
                "may not re-mint it from the graph projection."
            ),
        )

    keys = tuple(dict.fromkeys(cognitive_source_semantic_key(r) for r in records))
    sealed = await lookup(context, board_id, keys)
    reconciled, restorations = restore_sealed_birth_fields(records, sealed or {})
    for restoration in restorations:
        # WARNING, not INFO: a restoration means the graph projection lost a
        # node the ledger still holds. The append is safe now, but the graph
        # carries a birth stamp that a rebuild will overwrite.
        logger.warning(
            "kg.cognitive_source.birth_stamp_restored board=%s session=%s "
            "node_id=%s field=%s rederived=%s sealed=%s",
            board_id,
            session_id,
            restoration.node_id,
            restoration.field,
            restoration.rederived,
            restoration.sealed,
            extra={
                "event": "kg.cognitive_source.birth_stamp_restored",
                "board_id": board_id,
                "session_id": session_id,
                "node_id": restoration.node_id,
                "node_type": restoration.node_type,
                "generation": restoration.generation,
                "field": restoration.field,
                "rederived_value": str(restoration.rederived),
                "sealed_value": str(restoration.sealed),
            },
        )
    return list(reconciled)


async def _append_cognitive_source_records(
    board_id: str,
    session_id: str,
    records: list[dict],
    *,
    context: object | None = None,
    store: object | None = None,
) -> None:
    """Append cognitive-source records fail-closed (spec MKG-A-S1 FR4/D5).

    Raises ``KGPrimitiveError`` with the stable code
    ``kg_cognitive_source_unavailable`` — the caller's coordinator compensates
    graph writes best-effort.  The graph scope is
    already closed when this function runs, so no relational wait can retain
    the process-global embedded writer.
    """

    if not records:
        return
    from okto_pulse.core.ports.kg_cognitive_source import (
        CognitiveSourceError,
        CognitiveSourceRecord,
        require_cognitive_source_store,
    )

    try:
        resolved_store = store or require_cognitive_source_store()
        if context is None:
            source_records = tuple(
                CognitiveSourceRecord(**kwargs) for kwargs in records
            )
            await resolved_store.append_many(source_records)
        else:
            append_in_context = getattr(
                resolved_store,
                "append_many_in_context",
                None,
            )
            if not callable(append_in_context):
                raise CognitiveSourceError(
                    "cognitive_source_context_append_unsupported",
                    board_id=board_id,
                    remediation=(
                        "Install a CognitiveSourceStore adapter that supports "
                        "the caller-owned relational unit of work."
                    ),
                )
            records = await _restore_sealed_birth_fields(
                board_id,
                session_id,
                records,
                context=context,
                store=resolved_store,
            )
            source_records = tuple(
                CognitiveSourceRecord(**kwargs) for kwargs in records
            )
            await append_in_context(context, source_records)
    except CognitiveSourceError as exc:
        # OR1: structured failure log — the operator must distinguish
        # "commit aborted: durable source unavailable" from graph failures.
        logger.error(
            "kg.cognitive_source.append_failed board=%s session=%s "
            "node_id=%s reason=%s",
            board_id,
            session_id,
            exc.node_id,
            exc.failure_reason,
            extra={
                "event": "kg.cognitive_source.append_failed",
                "board_id": board_id,
                "session_id": session_id,
                "node_id": exc.node_id,
                "failure_reason": exc.failure_reason,
            },
        )
        raise KGPrimitiveError(
            "kg_cognitive_source_unavailable",
            "cognitive durable-source append failed; commit aborted "
            "fail-closed and graph compensation was requested. Retry after "
            "the application database recovers; identical appends are "
            "idempotent per (node_id, generation, source_revision), while "
            "divergent replays "
            "are rejected as integrity conflicts.",
            session_id=session_id,
            details={
                "board_id": board_id,
                "failure_reason": exc.failure_reason,
                "node_id": exc.node_id,
            },
        ) from exc
    except Exception as exc:
        node_id = str(records[0].get("node_id") or "")
        logger.exception(
            "kg.cognitive_source.append_failed board=%s session=%s "
            "node_id=%s reason=cognitive_source_append_unexpected",
            board_id,
            session_id,
            node_id,
            extra={
                "event": "kg.cognitive_source.append_failed",
                "board_id": board_id,
                "session_id": session_id,
                "node_id": node_id,
                "failure_reason": "cognitive_source_append_unexpected",
            },
        )
        raise KGPrimitiveError(
            "kg_cognitive_source_unavailable",
            "cognitive durable-source append failed unexpectedly; commit "
            "aborted fail-closed and graph compensation was requested.",
            session_id=session_id,
            details={
                "board_id": board_id,
                "failure_reason": "cognitive_source_append_unexpected",
                "node_id": node_id,
                "error_type": type(exc).__name__,
            },
        ) from exc


def _node_semantic_fields(
    graph_scope,
    node_type: str,
    node_id: str,
) -> dict[str, str | None] | None:
    """Read assertion-bearing fields used by the Decision lineage policy."""

    if not node_id:
        return None
    try:
        result = graph_scope.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id "
            "RETURN n.title, n.content, n.context, n.justification LIMIT 1",
            {"id": node_id},
        )
        rows = getattr(result, "rows", ())
        if rows:
            row = rows[0]
            return {
                "title": row[0],
                "content": row[1],
                "context": row[2],
                "justification": row[3],
            }
    except Exception:
        pass
    return None


def _node_title(graph_scope, node_type: str, node_id: str) -> str:
    """Read a node's current title (spec MKG-D-S1 FR8 trail criterion).

    Returns "" on missing node or read error — an unreadable title then
    compares as different only when the candidate title is non-empty,
    which errs on the side of preserving history.
    """
    if not node_id:
        return ""
    cypher = f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.title LIMIT 1"
    try:
        res = graph_scope.execute(cypher, {"id": node_id})
        if res.rows:
            value = res.rows[0][0]
            return str(value) if value is not None else ""
    except Exception:
        pass
    return ""


def _node_superseded_by(graph_scope, node_type: str, node_id: str) -> str | None:
    """Return the deterministic successor linked from ``node_id``, if any."""

    if not node_id:
        return None
    try:
        result = graph_scope.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.superseded_by LIMIT 1",
            {"id": node_id},
        )
        rows = getattr(result, "rows", ())
        if rows and rows[0][0]:
            return str(rows[0][0])
    except Exception:
        pass
    return None


def _node_generation(graph_scope, node_type: str, node_id: str) -> int:
    """Read a node's supersedence generation (spec MKG-A-S1 FR3).

    Treats NULL as 0 — legacy nodes from before the generation column have
    no value and start the deterministic chain at generation 0. Returns 0
    on any read error so a supersede of a legacy node still mints
    generation 1 deterministically instead of failing the commit.
    """
    if not node_id:
        return 0
    cypher = f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n.generation LIMIT 1"
    try:
        res = graph_scope.execute(cypher, {"id": node_id})
        if res.rows:
            value = res.rows[0][0]
            return int(value) if value is not None else 0
    except Exception:
        pass
    return 0


_CROSS_SESSION_PREFIXES: tuple[str, ...] = (
    "story_",
    "ideation_",
    "refinement_",
    "spec_",
    "sprint_",
    "card_",
)
_SOURCE_REF_ENDPOINT_PREFIX = "kgref:"


def _parse_source_ref_endpoint(endpoint: str) -> tuple[str, str] | None:
    """Parse a closed physical-node/source-artifact cross-session reference."""

    if not endpoint.startswith(_SOURCE_REF_ENDPOINT_PREFIX):
        return None
    remainder = endpoint[len(_SOURCE_REF_ENDPOINT_PREFIX) :]
    node_type, separator, source_artifact_ref = remainder.partition(":")
    if not separator or not source_artifact_ref or len(source_artifact_ref) > 4096:
        return None
    from okto_pulse.core.kg.schema_contract import NODE_TYPES

    if node_type not in NODE_TYPES:
        return None
    return node_type, source_artifact_ref


def _is_spec_root_source_ref(source_artifact_ref: str) -> bool:
    """Return whether a source ref identifies a Spec root, not a child node."""

    prefix, separator, artifact_id = str(source_artifact_ref or "").partition(":")
    return bool(
        prefix == "spec" and separator and artifact_id and ":" not in artifact_id
    )


def _resolve_spec_dependency_endpoints(
    *,
    projection_intent: object | None,
    edge_candidates: dict[str, object],
    session_id: str,
    graph_scope: object,
) -> dict[str, tuple[str, str]]:
    """Resolve every active PRECEDES endpoint before the first graph write.

    Queue order is only a convergence hint: a dependent job can be claimed
    before a newly-created prerequisite. In that case reject this attempt as
    pending while the graph scope is still read-only. The normal retry then
    commits the exact active set once prerequisite consolidation materializes
    the canonical root; no partial owner node or compensation cycle is created.
    """

    if (
        projection_intent is None
        or str(getattr(projection_intent, "namespace", "")) != "dependencies"
    ):
        return {}

    active_edge_refs = tuple(getattr(projection_intent, "active_edges", ()))
    declared_ids = {str(getattr(ref, "candidate_id", "")) for ref in active_edge_refs}
    emitted_ids = {
        candidate_id
        for candidate_id, candidate in edge_candidates.items()
        if str(getattr(candidate, "rule_id", "") or "").startswith(
            "precedes/spec_dependency/"
        )
    }
    if (
        "" in declared_ids
        or len(declared_ids) != len(active_edge_refs)
        or declared_ids != emitted_ids
    ):
        raise KGPrimitiveError(
            "relational_projection_active_set_mismatch",
            "Projection edge candidates must exactly match the active set.",
            session_id=session_id,
        )

    resolved: dict[str, tuple[str, str]] = {}
    desired_endpoints: set[tuple[str, str, str]] = set()
    for edge_ref in active_edge_refs:
        edge_candidate_id = str(getattr(edge_ref, "candidate_id", ""))
        candidate = edge_candidates.get(edge_candidate_id)
        endpoint = str(getattr(edge_ref, "from_candidate_id", ""))
        endpoint_identity = (
            str(getattr(edge_ref, "edge_type", "")),
            endpoint,
            str(getattr(edge_ref, "to_candidate_id", "")),
        )
        parsed = _parse_source_ref_endpoint(endpoint)
        if candidate is None or (
            _enum_value(candidate.edge_type) != str(getattr(edge_ref, "edge_type", ""))
            or candidate.from_candidate_id != endpoint
            or candidate.to_candidate_id != endpoint_identity[2]
            or str(candidate.rule_id or "") != str(getattr(edge_ref, "rule_id", ""))
        ):
            raise KGPrimitiveError(
                "relational_projection_edge_identity_mismatch",
                "An active relational edge changed after session admission.",
                session_id=session_id,
            )
        if (
            parsed is None
            or parsed[0] != "Entity"
            or not _is_spec_root_source_ref(parsed[1])
            or endpoint_identity in desired_endpoints
        ):
            raise KGPrimitiveError(
                "relational_projection_endpoint_invalid",
                "A Spec dependency endpoint is outside its exact projection scope.",
                session_id=session_id,
                details={"edge_candidate_id": edge_candidate_id},
            )
        desired_endpoints.add(endpoint_identity)
        try:
            node_id, node_type = _resolve_endpoint(
                endpoint,
                {},
                graph_scope=graph_scope,
            )
        except Exception as exc:
            raise KGPrimitiveError(
                "relational_projection_endpoint_lookup_failed",
                "A Spec dependency endpoint could not be resolved safely.",
                session_id=session_id,
                details={
                    "edge_candidate_id": edge_candidate_id,
                    "failure_type": type(exc).__name__,
                },
            ) from exc
        if node_id is None:
            raise KGPrimitiveError(
                "relational_projection_endpoint_pending",
                "A prerequisite Spec root is not materialized yet; retry the "
                "dependent relational projection after prerequisite consolidation.",
                session_id=session_id,
                details={
                    "edge_candidate_id": edge_candidate_id,
                    "source_artifact_ref": parsed[1],
                },
            )
        resolved[endpoint] = (node_id, node_type or parsed[0])
    return resolved


def _is_cross_session_entity_ref(endpoint: str) -> bool:
    """Match the `<artifact_type>_<short>_entity` shape Layer 1 emits when
    referencing a parent Entity committed by an earlier session.

    The validation in `add_edge_candidate` accepts these as deferred
    endpoints; `_resolve_endpoint` later does the actual graph backend lookup at
    commit time.
    """
    if not endpoint.endswith("_entity"):
        return False
    body = endpoint[: -len("_entity")]
    return any(
        body.startswith(p) and len(body) > len(p) for p in _CROSS_SESSION_PREFIXES
    )


def _resolve_endpoint(
    endpoint: str,
    candidate_to_graph_id: dict[str, str],
    *,
    graph_scope=None,
) -> tuple[str | None, str | None]:
    """Resolve an edge endpoint to an existing or newly-created graph_node_id.

    Returns ``(node_id, node_type)``. ``node_type`` is non-None only when the
    resolution required a graph backend lookup (cross-session ref) — single-session
    candidates are typed by the caller via the local NodeCandidate.

    Resolution order:
        1. ``kg:<id>`` literal — strip prefix and trust the caller.
        2. Local session candidate — match by candidate_id in the supplied map.
        3. ``kgref:<PhysicalType>:<source_artifact_ref>`` — exact,
           allowlisted physical-type lookup used by relational projections.
        4. Cross-session by deterministic id pattern (`spec_<short>_entity` /
           `sprint_<short>_entity`) — derive ``source_artifact_ref`` and
           probe graph backend for an Entity with that ref. Used by Layer 1 to wire
           Sprint→Spec / Card→Sprint hierarchy edges across sessions.
    """
    if endpoint.startswith("kg:"):
        node_id = endpoint[3:]
        if graph_scope is not None:
            return node_id, _lookup_node_type_by_id(graph_scope, node_id)
        return node_id, None
    local = candidate_to_graph_id.get(endpoint)
    if local is not None:
        return local, None
    if graph_scope is None:
        return None, None
    source_ref_endpoint = _parse_source_ref_endpoint(endpoint)
    if source_ref_endpoint is not None:
        node_type, source_artifact_ref = source_ref_endpoint
        result = graph_scope.execute(
            f"MATCH (n:{node_type}) "
            "WHERE n.source_artifact_ref = $ref "
            "AND n.superseded_by IS NULL "
            "RETURN n.id LIMIT 2",
            {"ref": source_artifact_ref},
        )
        rows = tuple(getattr(result, "rows", ()) or ())
        if len(rows) > 1:
            raise ValueError("source_ref_endpoint_ambiguous")
        if rows:
            return str(rows[0][0]), node_type
        return None, node_type
    # Cross-session deterministic-id fallback. We only handle the worker's
    # own naming convention here (`<artifact>_<id8>_entity`) to avoid
    # surprises; new patterns must be opt-in.
    if endpoint.endswith("_entity"):
        body = endpoint[: -len("_entity")]
        for prefix, ref_prefix in (
            ("story_", "story:"),
            ("ideation_", "ideation:"),
            ("refinement_", "refinement:"),
            ("spec_", "spec:"),
            ("sprint_", "sprint:"),
            ("card_", "card:"),
        ):
            if body.startswith(prefix):
                short = body[len(prefix) :]
                # Source_artifact_ref uses the full UUID. We probe with a
                # prefix match because the worker only carries the first 8
                # chars in the candidate id.
                cypher = (
                    "MATCH (n:Entity) "
                    "WHERE n.source_artifact_ref STARTS WITH $ref "
                    "RETURN n.id LIMIT 1"
                )
                try:
                    res = graph_scope.execute(cypher, {"ref": f"{ref_prefix}{short}"})
                    if res.rows:
                        return res.rows[0][0], "Entity"
                except Exception:
                    pass
                break
    return None, None


_NODE_UPDATEABLE_ATTRS: frozenset[str] = frozenset(
    {
        "title",
        "content",
        "context",
        "justification",
        "priority_boost",
        "source_confidence",
        # Maturity METADATA (card 302044a7 / FR4 / dec_85ba8dc2): graph_layer +
        # maturity_status are safe to PROMOTE on a merge by source_artifact_ref
        # (e.g. working->canonical when the source spec reaches done). They are
        # maturity metadata, NOT curated content — so they update even for
        # human_curated nodes, while title/content/context/justification stay
        # protected. Historical/HNSW-locked fields (embedding, created_at,
        # query_hits, human_curated, …) remain excluded.
        "graph_layer",
        "maturity_status",
        # Spec MKG-B-S1 (FR3/D5): extraction provenance is CONTENT-DERIVED —
        # when the NC-8 rewrite replaces the content, the anchor must describe
        # the new assertion (drift clears after re-consolidation). Attestation
        # counters stay EXCLUDED: they accumulate via _bump_attestation and can
        # never be reset by an update payload.
        "source_span_start",
        "source_span_end",
        "source_span_quote",
        "extraction_model_id",
        "extraction_prompt_hash",
        "source_content_hash",
        # Spec MKG-E-S1 (FR4): the declared subtype is content-derived too.
        "kind_of",
        # Code Traceability is a deterministic relational projection.  Its
        # optional metadata follows the immutable source row on a refresh;
        # no external repository is consulted at this boundary.
        "investigation_receipt_id",
        "source_ref",
        "attestor_actor_id",
        "declared_revision",
        "workspace_state_id",
        "code_path",
        "symbol_qualified_name",
        "symbol_kind",
        "selector_kind",
        "selector_fingerprint",
        "resolution_state",
    }
)

# Vector values are adapter-managed immutable attributes on the incremental
# dedup path. Content metadata can be refreshed without requiring the Core to
# understand index maintenance capabilities.


def _apply_graph_node_update_partial(
    orch, node_type: str, node_id: str, attrs: dict
) -> None:
    """Update the backend-neutral mutable subset of an existing node.

    Used by `_do_graph_commit` when source_artifact_ref already maps to a
    node — preserves historical fields (created_at, created_by_agent,
    query_hits, last_queried_at, relevance_score, source_session_id,
    human_curated) and refreshes only content-derived attrs.

    Filters input via `_NODE_UPDATEABLE_ATTRS` to ensure no historical
    field can be accidentally clobbered if the caller passes extras —
    notably backend-managed vectors and historical timestamps/counters.
    """
    values = {
        key: value for key, value in attrs.items() if key in _NODE_UPDATEABLE_ATTRS
    }
    if not values:
        return
    orch.update_node(
        node_type,
        node_id,
        values,
        count_candidate=False,
    )


def _apply_graph_node_create(orch, node_type: str, node_id: str, attrs: dict) -> None:
    """Create a node through the semantic transaction scope."""
    orch.create_node(node_type, node_id, attrs)


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
        _validate_session_state(session, allow_pending_commit=False)
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


async def _get_latest_audit(
    registry,
    db,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
):
    """Get latest committed audit via the composed AuditRepository."""
    if registry.audit_repo is not None:
        return await registry.audit_repo.get_latest_for_artifact(
            board_id,
            artifact_id,
            artifact_type=artifact_type,
        )
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


async def _commit_audit_records(
    registry, db, records, counters, req, session, agent_id, committed_at
):
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

    graph_refs = [
        NodeRefData(
            session_id=req.session_id,
            board_id=session.board_id,
            graph_node_id=r.entity_id,
            graph_node_type=r.entity_type,
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
        await registry.audit_repo.stage_consolidation_records(
            db,
            audit_data,
            graph_refs,
            outbox_data,
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
