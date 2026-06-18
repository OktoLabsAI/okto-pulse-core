"""Background worker that drains the consolidation_queue (spec c48a5c33).

For each pending queue entry the worker:
    1. Loads the artifact (Spec/Sprint/Card) from the DB.
    2. Runs the pure `DeterministicWorker` (Layer 1) to extract every
       node + edge candidate that can be derived from structured fields,
       with full v0.2.0 provenance metadata (layer/rule_id/created_by).
    3. Drives the primitives pipeline: begin → propose_reconciliation →
       commit. The session uses `agent_id="system:historical_consolidation"`
       so the layer-ownership BR allows deterministic edges through.
    4. Runs the LadybugDB safe-write lifecycle before the queue row is
       acknowledged, proving the graph is readable from disk after
       close/reopen.
    5. Marks the queue entry as `done` (or `failed`).

The cognitive agent picks up `missing_link_candidates` later and proposes
the residual semantic edges (capped at confidence 0.85 per BR `Cognitive
Fallback Confidence Cap`).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from okto_pulse.core.models.db import (
    Card,
    ConsolidationQueue,
    Ideation,
    Refinement,
    Spec,
    Sprint,
    Story,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    EdgeCandidate,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.primitives import (
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.memory_pressure import FailureEvent
from okto_pulse.core.kg.memory_pressure_collector import record_failure
from okto_pulse.core.kg.workers.dead_letter import route_to_dead_letter
from okto_pulse.core.kg.schema_layer_guard import (
    ensure_graph_layer_schema,
    is_graph_layer_schema_error,
)
from okto_pulse.core.kg.safe_write_lifecycle import (
    STEP_CHECKPOINT,
    STEP_FLUSH,
    STEP_FSYNC,
    HealthProbe,
    KGSafeWriteLifecycle,
    LockOwnerProbe,
    SafeWriteLifecycleStatus,
)
from okto_pulse.core.kg.schema import apply_ladybug_lifecycle_step
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_NONE,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    classify_source_for_kg,
)
from okto_pulse.core.kg.write_barrier import under_safe_write
from okto_pulse.core.kg.workers.deterministic_worker import (
    DeterministicWorker,
    EmittedEdge,
    EmittedNode,
    WORKER_VERSION,
    WorkerResult,
    _spec_child_ref,
)
from okto_pulse.core.services.canonical_debt_service import (
    mark_canonical_debt_committed_for_artifact,
    upsert_canonical_debt,
)

logger = logging.getLogger("okto_pulse.kg.consolidation_worker")

AGENT_ID = "system:historical_consolidation"
CONSOLIDATION_COMMIT_OPERATION = "consolidation_worker_commit"

# Spec 3d89c192 (FR-4): o commit incremental do worker usa o subset
# não-destrutivo do lifecycle — checkpoint real + verificação + fsync, SEM o
# close_reopen_probe. O probe fecha o Database compartilhado (use-after-close
# com leitores concorrentes) e só é necessário nas lanes de rebuild/recovery,
# que continuam usando DEFAULT_REQUIRED_STEPS (contrato api_1c9d19e1 prevê
# subset custom por caller).
WORKER_COMMIT_LIFECYCLE_STEPS: tuple[str, ...] = (
    STEP_CHECKPOINT,
    STEP_FLUSH,
    STEP_FSYNC,
)

_board_processing_locks: dict[str, asyncio.Lock] = {}


def _get_board_processing_lock(board_id: str) -> asyncio.Lock:
    lock = _board_processing_locks.get(board_id)
    if lock is None:
        lock = asyncio.Lock()
        _board_processing_locks[board_id] = lock
    return lock


def _worker_owner_probe(_board_id: str, owner_token: str) -> bool:
    """Validate process-local consolidation owner tokens.

    The historical consolidation worker is already serialised by the
    queue-claim contract for one board inside one server process. The
    critical durability gap was that normal worker commits never entered
    the safe-write guard/lifecycle before acknowledging the queue row.
    This probe keeps KGSafeWriteLifecycle's owner-token contract explicit
    for the worker-owned token generated per queue entry.
    """

    return owner_token.startswith("consolidation-worker:")


def _worker_health_probe(
    _board_id: str,
    _graph_type: str,
    status: SafeWriteLifecycleStatus,
    _step: str | None,
) -> str:
    return "healthy" if status is SafeWriteLifecycleStatus.APPLIED else "recovery_needed"


def _apply_board_graph_lifecycle_after_commit(
    *,
    board_id: str,
    owner_token: str,
    mutation_ref: str,
):
    """Run checkpoint/flush/fsync/close-reopen before queue acknowledgement.

    FR3 (spec R2c): when the lifecycle fails, a FailureEvent with
    ``event_kind="kg.wal.flush.failed"`` is recorded in the collector
    ring-buffer before the RuntimeError is re-raised.  This is
    non-blocking (the record call is swallowed if it fails) so it never
    masks the original lifecycle error.
    """

    lifecycle = KGSafeWriteLifecycle(
        step_adapter=apply_ladybug_lifecycle_step,
        owner_probe=LockOwnerProbe(is_active_owner=_worker_owner_probe),
        health_probe=HealthProbe(classify=_worker_health_probe),
    )
    response = lifecycle.apply(
        board_id=board_id,
        graph_type="board_graph",
        operation=CONSOLIDATION_COMMIT_OPERATION,
        owner_token=owner_token,
        mutation_ref=mutation_ref,
        required_steps=WORKER_COMMIT_LIFECYCLE_STEPS,
    )
    if response.status is not SafeWriteLifecycleStatus.APPLIED:
        # FR3: record WAL/lifecycle failure before raising so the correlator
        # receives a real FailureEvent.  Swallow any collector error (TR2).
        try:
            record_failure(
                board_id,
                FailureEvent(
                    timestamp=datetime.now(timezone.utc),
                    event_kind="kg.wal.flush.failed",
                    graph_type="board",
                    correlation_id=uuid.uuid4().hex,
                ),
            )
        except Exception:
            pass
        raise RuntimeError(
            "board_graph_safe_lifecycle_failed "
            f"board_id={board_id} mutation_ref={mutation_ref} "
            f"failed_step={response.failed_step} "
            f"health_state_after={response.health_state_after} "
            f"correlation_id={response.correlation_id}"
        )
    return response


async def _commit_consolidation_with_board_graph_lifecycle(
    *,
    entry: ConsolidationQueue,
    session_id: str,
    summary_text: str,
    db: AsyncSession,
):
    """Commit a queue item and prove the persisted graph before ACK.

    A previous implementation called ``commit_consolidation`` directly and
    then deleted the queue row. Field evidence showed the graph could be
    readable through the process handle while reopening after restart
    produced an empty/corrupt graph. This wrapper makes the ACK depend on
    the same LadybugDB lifecycle used by explicit rebuild recovery.
    """

    owner_token = f"consolidation-worker:{entry.id}:{uuid.uuid4().hex}"
    mutation_ref = f"{entry.artifact_type}:{entry.artifact_id}:{session_id}"
    with under_safe_write(entry.board_id, owner_token, CONSOLIDATION_COMMIT_OPERATION):
        commit_resp = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=session_id,
                summary_text=summary_text,
            ),
            agent_id=AGENT_ID,
            db=db,
        )
        # Em thread (review dcea02d, blocker): o lifecycle é síncrono —
        # CHECKPOINT, fsync e o dreno da higiene (até 2s sob leitor ativo)
        # bloqueavam o event loop INTEIRO a cada commit durante janelas de
        # scan. asyncio.to_thread copia contextvars, então o write barrier
        # de under_safe_write continua visível para o owner probe.
        await asyncio.to_thread(
            _apply_board_graph_lifecycle_after_commit,
            board_id=entry.board_id,
            owner_token=owner_token,
            mutation_ref=mutation_ref,
        )
    return commit_resp


async def _process_queue_entry_serialized(
    db: AsyncSession,
    entry: ConsolidationQueue,
) -> bool:
    """Process one queue row under a process-local per-board mutex.

    The queue claim contract prevents duplicate rows, but reprocess tools,
    background workers and rebuild waiters can instantiate more than one
    worker object in the same server process. LadybugDB allows only one
    write transaction per graph. Without this guard, two workers can claim
    different rows for the same board and collide at commit time.
    """

    async with _get_board_processing_lock(entry.board_id):
        return await _process_queue_entry(db, entry)


# ---------------------------------------------------------------------------
# Adapter: SQLAlchemy artifact → DeterministicWorker dict shape
# ---------------------------------------------------------------------------


def _architecture_design_to_dict(design) -> dict:
    return {
        "id": design.id,
        "title": design.title,
        "global_description": design.global_description,
        "entities": design.entities or [],
        "interfaces": design.interfaces or [],
        "diagrams": design.diagrams or [],
        "version": design.version,
        "source_ref": design.source_ref,
        "source_version": design.source_version,
    }


def _spec_to_dict(spec: Spec) -> dict:
    """Serialise a Spec row into the dict shape DeterministicWorker expects.
    Mirrors the JSON emitted by the Spec API routes so unit tests run under
    the same contract as production callers."""
    return {
        "id": spec.id,
        "board_id": spec.board_id,
        "ideation_id": getattr(spec, "ideation_id", None),
        "refinement_id": getattr(spec, "refinement_id", None),
        "title": spec.title,
        "description": spec.description,
        "context": spec.context,
        "status": getattr(getattr(spec, "status", None), "value", getattr(spec, "status", None)),
        "functional_requirements": spec.functional_requirements or [],
        "technical_requirements": spec.technical_requirements or [],
        "acceptance_criteria": spec.acceptance_criteria or [],
        "business_rules": spec.business_rules or [],
        "test_scenarios": spec.test_scenarios or [],
        "api_contracts": spec.api_contracts or [],
        "integration_requirements": getattr(spec, "integration_requirements", None) or [],
        "observability_requirements": getattr(spec, "observability_requirements", None) or [],
        "decisions": spec.decisions or [],
        "architecture_designs": [
            _architecture_design_to_dict(design)
            for design in (getattr(spec, "architecture_designs", None) or [])
        ],
    }


def _story_to_dict(story: Story) -> dict:
    status = getattr(story, "status", None)
    return {
        "id": story.id,
        "board_id": story.board_id,
        "topic_id": story.topic_id,
        "title": story.title,
        "description": story.description,
        "actor": story.actor,
        "goal": story.goal,
        "benefit": story.benefit,
        "labels": story.labels or [],
        "status": getattr(status, "value", status) if status is not None else None,
    }


def _ideation_to_dict(ideation: Ideation) -> dict:
    status = getattr(ideation, "status", None)
    complexity = getattr(ideation, "complexity", None)
    return {
        "id": ideation.id,
        "board_id": ideation.board_id,
        "title": ideation.title,
        "description": ideation.description,
        "problem_statement": ideation.problem_statement,
        "proposed_approach": ideation.proposed_approach,
        "scope_assessment": ideation.scope_assessment or {},
        "complexity": getattr(complexity, "value", complexity) if complexity is not None else None,
        "status": getattr(status, "value", status) if status is not None else None,
        "labels": ideation.labels or [],
        "story_ids": [
            link.story_id
            for link in (getattr(ideation, "story_links", None) or [])
            if getattr(link, "story_id", None)
        ],
    }


def _refinement_to_dict(refinement: Refinement) -> dict:
    status = getattr(refinement, "status", None)
    return {
        "id": refinement.id,
        "board_id": refinement.board_id,
        "ideation_id": refinement.ideation_id,
        "title": refinement.title,
        "description": refinement.description,
        "in_scope": refinement.in_scope or [],
        "out_of_scope": refinement.out_of_scope or [],
        "analysis": refinement.analysis,
        "decisions": refinement.decisions or [],
        "status": getattr(status, "value", status) if status is not None else None,
        "labels": refinement.labels or [],
    }


def _sprint_to_dict(sprint: Sprint) -> dict:
    return {
        "id": sprint.id,
        "board_id": sprint.board_id,
        "title": sprint.title,
        "description": sprint.description,
        "objective": sprint.objective,
        "expected_outcome": sprint.expected_outcome,
        "status": getattr(getattr(sprint, "status", None), "value", getattr(sprint, "status", None)),
        "spec_id": sprint.spec_id,
        "lane_type": getattr(getattr(sprint, "lane_type", None), "value", getattr(sprint, "lane_type", None)) or "normal",
        "origin_sprint_id": getattr(sprint, "origin_sprint_id", None),
        "origin_bug_id": getattr(sprint, "origin_bug_id", None),
    }


def _card_to_dict(card) -> dict:
    priority = getattr(card, "priority", None)
    severity = getattr(card, "severity", None)
    return {
        "id": card.id,
        "board_id": card.board_id,
        "title": card.title,
        "description": card.description,
        "status": getattr(getattr(card, "status", None), "value", getattr(card, "status", None)),
        "card_type": getattr(card.card_type, "value", card.card_type) if getattr(card, "card_type", None) else "normal",
        "spec_id": card.spec_id,
        "sprint_id": card.sprint_id,
        "origin_task_id": getattr(card, "origin_task_id", None),
        "linked_test_task_ids": getattr(card, "linked_test_task_ids", None) or [],
        "priority": getattr(priority, "value", priority) if priority is not None else None,
        "severity": getattr(severity, "value", severity) if severity is not None else None,
        "has_minimal_evidence": _card_has_minimal_evidence(card),
        "architecture_designs": [
            _architecture_design_to_dict(design)
            for design in (getattr(card, "architecture_designs", None) or [])
        ],
    }


def _worker_node_to_candidate(node: EmittedNode) -> NodeCandidate:
    return NodeCandidate(
        candidate_id=node.candidate_id,
        node_type=KGNodeType(node.node_type),
        title=node.title,
        content=node.content,
        context=node.context or None,
        source_artifact_ref=node.source_artifact_ref,
        graph_layer=node.graph_layer,
        maturity_status=node.maturity_status,
        source_confidence=node.source_confidence,
        priority_boost=node.priority_boost,
    )


def _layer_attrs_for_artifact(
    artifact_type: str,
    status: Any,
    *,
    has_minimal_evidence: bool = True,
) -> tuple[str, str]:
    classification = classify_source_for_kg(
        artifact_type=artifact_type,
        artifact_status=status,
        content_hash="consolidation-lineage",
        has_minimal_evidence=has_minimal_evidence,
    )
    graph_layer = classification.graph_layer
    if graph_layer == GRAPH_LAYER_NONE:
        graph_layer = GRAPH_LAYER_WORKING
    return graph_layer, classification.maturity_status


def _card_source_artifact_type(card_type: Any) -> str:
    normalized = str(card_type or "normal").lower()
    if normalized == "test":
        return "test"
    if normalized == "bug":
        return "bug"
    return "task"


def _card_has_minimal_evidence(card: Card) -> bool:
    card_type = getattr(card.card_type, "value", card.card_type) if card.card_type else "normal"
    if card_type != "bug":
        return True
    has_text = any(
        str(getattr(card, field, "") or "").strip()
        for field in ("observed_behavior", "expected_behavior", "steps_to_reproduce")
    )
    return has_text and (
        bool(getattr(card, "linked_test_task_ids", None))
        or bool(getattr(card, "conclusions", None))
    )


def _worker_edge_to_candidate(edge: EmittedEdge) -> EdgeCandidate:
    return EdgeCandidate(
        candidate_id=edge.candidate_id,
        edge_type=KGEdgeType(edge.edge_type),
        from_candidate_id=edge.from_candidate_id,
        to_candidate_id=edge.to_candidate_id,
        confidence=edge.confidence,
        layer=edge.layer,
        rule_id=edge.rule_id,
        created_by=edge.created_by,
        fallback_reason=edge.fallback_reason or None,
    )


def _run_deterministic_worker(entry: ConsolidationQueue, artifact) -> WorkerResult:
    worker = DeterministicWorker()
    if entry.artifact_type == "story":
        return worker.process_story(_story_to_dict(artifact))
    if entry.artifact_type == "ideation":
        return worker.process_ideation(_ideation_to_dict(artifact))
    if entry.artifact_type == "refinement":
        return worker.process_refinement(_refinement_to_dict(artifact))
    if entry.artifact_type == "spec":
        return worker.process_spec(_spec_to_dict(artifact))
    if entry.artifact_type == "sprint":
        return worker.process_sprint(_sprint_to_dict(artifact))
    if entry.artifact_type == "card":
        return worker.process_card(_card_to_dict(artifact))
    raise ValueError(f"unknown artifact_type: {entry.artifact_type}")


def _edge_exists(result: WorkerResult, candidate_id: str) -> bool:
    return any(edge.candidate_id == candidate_id for edge in result.edges)


def _node_exists(result: WorkerResult, candidate_id: str) -> bool:
    return any(node.candidate_id == candidate_id for node in result.nodes)


def _append_card_entity_node(result: WorkerResult, card: Card) -> str:
    cid = f"card_{card.id[:8]}_entity"
    card_type = getattr(card.card_type, "value", card.card_type) if card.card_type else "normal"
    if not _node_exists(result, cid):
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            _card_source_artifact_type(card_type),
            getattr(getattr(card, "status", None), "value", getattr(card, "status", None)),
            has_minimal_evidence=_card_has_minimal_evidence(card),
        )
        result.nodes.append(EmittedNode(
            candidate_id=cid,
            node_type="Bug" if card_type == "bug" else "Entity",
            title=card.title or f"Card {card.id}",
            content=card.description or "",
            source_artifact_ref=f"card:{card.id}",
            graph_layer=graph_layer,
            maturity_status=maturity_status,
            source_confidence=1.0,
        ))
    if getattr(card, "board_id", None):
        _attach_entity_node_to_board_root(
            result,
            board_id=card.board_id,
            child_candidate_id=cid,
            rule_slot="card",
        )
    return cid


def _append_spec_entity_node(result: WorkerResult, spec: Spec) -> str:
    cid = f"spec_{spec.id[:8]}_entity"
    if not _node_exists(result, cid):
        content = "\n\n".join(
            p for p in (
                getattr(spec, "description", None),
                getattr(spec, "context", None),
            )
            if p
        )
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "spec",
            getattr(getattr(spec, "status", None), "value", getattr(spec, "status", None)),
        )
        result.nodes.append(EmittedNode(
            candidate_id=cid,
            node_type="Entity",
            title=getattr(spec, "title", None) or f"Spec {spec.id}",
            content=content or getattr(spec, "title", None) or "",
            source_artifact_ref=f"spec:{spec.id}",
            graph_layer=graph_layer,
            maturity_status=maturity_status,
            source_confidence=1.0,
        ))
    if getattr(spec, "board_id", None):
        _attach_entity_node_to_board_root(
            result,
            board_id=spec.board_id,
            child_candidate_id=cid,
            rule_slot="spec",
        )
    return cid


def _append_story_entity_node(result: WorkerResult, story: Story) -> str:
    cid = f"story_{story.id[:8]}_entity"
    if _node_exists(result, cid):
        return cid
    graph_layer, maturity_status = _layer_attrs_for_artifact(
        "story",
        getattr(getattr(story, "status", None), "value", getattr(story, "status", None)),
    )
    result.nodes.append(EmittedNode(
        candidate_id=cid,
        node_type="Entity",
        title=story.title or f"Story {story.id}",
        content=story.description or "",
        source_artifact_ref=f"story:{story.id}",
        graph_layer=graph_layer,
        maturity_status=maturity_status,
        source_confidence=1.0,
    ))
    return cid


def _append_ideation_entity_node(result: WorkerResult, ideation: Ideation) -> str:
    cid = f"ideation_{ideation.id[:8]}_entity"
    if _node_exists(result, cid):
        return cid
    content = "\n\n".join(
        p for p in (
            ideation.description,
            ideation.problem_statement,
            ideation.proposed_approach,
        )
        if p
    )
    graph_layer, maturity_status = _layer_attrs_for_artifact(
        "ideation",
        getattr(getattr(ideation, "status", None), "value", getattr(ideation, "status", None)),
    )
    result.nodes.append(EmittedNode(
        candidate_id=cid,
        node_type="Entity",
        title=ideation.title or f"Ideation {ideation.id}",
        content=content or ideation.title or "",
        source_artifact_ref=f"ideation:{ideation.id}",
        graph_layer=graph_layer,
        maturity_status=maturity_status,
        source_confidence=1.0,
    ))
    return cid


def _append_refinement_entity_node(result: WorkerResult, refinement: Refinement) -> str:
    cid = f"refinement_{refinement.id[:8]}_entity"
    if _node_exists(result, cid):
        return cid
    content = "\n\n".join(
        p for p in (refinement.description, refinement.analysis) if p
    )
    graph_layer, maturity_status = _layer_attrs_for_artifact(
        "refinement",
        getattr(getattr(refinement, "status", None), "value", getattr(refinement, "status", None)),
    )
    result.nodes.append(EmittedNode(
        candidate_id=cid,
        node_type="Entity",
        title=refinement.title or f"Refinement {refinement.id}",
        content=content or refinement.title or "",
        source_artifact_ref=f"refinement:{refinement.id}",
        graph_layer=graph_layer,
        maturity_status=maturity_status,
        source_confidence=1.0,
    ))
    return cid


def _board_root_candidate_id(board_id: str) -> str:
    return f"board_{board_id[:8]}_entity"


def _append_board_root_entity_node(result: WorkerResult, board_id: str) -> str:
    cid = _board_root_candidate_id(board_id)
    if _node_exists(result, cid):
        return cid
    result.nodes.append(EmittedNode(
        candidate_id=cid,
        node_type="Entity",
        title=f"Board {board_id}",
        content="Deterministic KG board root.",
        source_artifact_ref=f"board:{board_id}",
        source_confidence=1.0,
    ))
    return cid


def _attach_entity_node_to_board_root(
    result: WorkerResult,
    *,
    board_id: str,
    child_candidate_id: str,
    rule_slot: str,
) -> None:
    board_root_id = _append_board_root_entity_node(result, board_id)
    edge_id = f"{child_candidate_id}_belongs_to_board"
    if _edge_exists(result, edge_id):
        return
    result.edges.append(EmittedEdge(
        candidate_id=edge_id,
        edge_type="belongs_to",
        from_candidate_id=child_candidate_id,
        to_candidate_id=board_root_id,
        confidence=1.0,
        rule_id=f"belongs_to/{rule_slot}_to_board@{WORKER_VERSION}",
    ))


async def _materialize_lineage_endpoint_nodes(
    db: AsyncSession,
    entry: ConsolidationQueue,
    artifact,
    result: WorkerResult,
) -> WorkerResult:
    """Add local parent/root nodes needed by deterministic lineage edges.

    Queue ordering is best-effort, but event-driven consolidation can process
    a child before its parent. Materialising the parent node in the same
    session keeps belongs_to edges from being silently skipped.
    """
    if entry.artifact_type == "ideation":
        story_ids = [
            link.story_id
            for link in (getattr(artifact, "story_links", None) or [])
            if getattr(link, "story_id", None)
        ]
        if story_ids:
            stories = (await db.execute(
                select(Story).where(Story.id.in_(story_ids))
            )).scalars().all()
            for story in stories:
                story_cid = _append_story_entity_node(result, story)
                _attach_entity_node_to_board_root(
                    result,
                    board_id=entry.board_id,
                    child_candidate_id=story_cid,
                    rule_slot="story",
                )
        return result

    if entry.artifact_type == "refinement" and getattr(artifact, "ideation_id", None):
        ideation = await db.get(Ideation, artifact.ideation_id)
        if ideation is not None:
            ideation_cid = _append_ideation_entity_node(result, ideation)
            _attach_entity_node_to_board_root(
                result,
                board_id=entry.board_id,
                child_candidate_id=ideation_cid,
                rule_slot="ideation",
            )
        return result

    if entry.artifact_type == "spec":
        if getattr(artifact, "refinement_id", None):
            refinement = await db.get(Refinement, artifact.refinement_id)
            if refinement is not None:
                refinement_cid = _append_refinement_entity_node(result, refinement)
                _attach_entity_node_to_board_root(
                    result,
                    board_id=entry.board_id,
                    child_candidate_id=refinement_cid,
                    rule_slot="refinement",
                )
        elif getattr(artifact, "ideation_id", None):
            ideation = await db.get(Ideation, artifact.ideation_id)
            if ideation is not None:
                ideation_cid = _append_ideation_entity_node(result, ideation)
                _attach_entity_node_to_board_root(
                    result,
                    board_id=entry.board_id,
                    child_candidate_id=ideation_cid,
                    rule_slot="ideation",
                )
        return result

    return result


def _test_scenario_content(ts: object) -> str:
    if not isinstance(ts, dict):
        return str(ts)
    parts = [
        f"Given: {ts.get('given', '')}",
        f"When: {ts.get('when', '')}",
        f"Then: {ts.get('then', '')}",
    ]
    return "\n".join(p for p in parts if p.split(": ", 1)[1])


def _scenario_candidate_for_id(spec: Spec, scenario_id: str) -> tuple[str, EmittedNode] | None:
    for index, ts in enumerate(spec.test_scenarios or []):
        if not isinstance(ts, dict):
            continue
        raw_id = ts.get("id") or ts.get("scenario_id")
        if str(raw_id) != str(scenario_id):
            continue
        cid = f"spec_{spec.id[:8]}_ts_{index}"
        title = ts.get("title") or f"TS-{index + 1}"
        return cid, EmittedNode(
            candidate_id=cid,
            node_type="TestScenario",
            title=title,
            content=_test_scenario_content(ts),
            source_artifact_ref=_spec_child_ref(spec.id, "test_scenario", ts, index),
            source_confidence=1.0,
        )
    return None


def _suggested_ref(candidate, prefix: str) -> str | None:
    for suggested in candidate.suggested_candidates or []:
        value = str(suggested)
        if value.startswith(prefix):
            return value.split(":", 1)[1]
    return None


async def _resolve_missing_link_candidates(
    db: AsyncSession,
    board_id: str,
    result: WorkerResult,
) -> WorkerResult:
    """Resolve structured cross-artifact Bug links before commit.

    The deterministic worker remains pure and emits MissingLinkCandidate rows
    when it sees a foreign key that requires repository lookup. This adapter
    turns the resolvable cases into final deterministic edges and keeps truly
    unresolved candidates available for audit/fallback.
    """
    origin_ids: set[str] = set()
    test_task_ids: set[str] = set()
    for candidate in result.missing_link_candidates:
        if candidate.reason == "origin_task_requires_cross_artifact_resolution":
            ref = _suggested_ref(candidate, "task:")
            if ref:
                origin_ids.add(ref)
        elif candidate.reason == "linked_test_task_requires_cross_artifact_resolution":
            ref = _suggested_ref(candidate, "test_task:")
            if ref:
                test_task_ids.add(ref)

    target_ids = origin_ids | test_task_ids
    if not target_ids:
        return result

    rows = (await db.execute(
        select(Card).where(Card.board_id == board_id, Card.id.in_(target_ids))
    )).scalars().all()
    cards_by_id = {card.id: card for card in rows}

    specs_by_id: dict[str, Spec] = {}
    spec_ids = {
        card.spec_id
        for card in cards_by_id.values()
        if card.id in test_task_ids and card.spec_id
    }
    if spec_ids:
        specs = (await db.execute(
            select(Spec).where(Spec.board_id == board_id, Spec.id.in_(spec_ids))
        )).scalars().all()
        specs_by_id = {spec.id: spec for spec in specs}

    unresolved = []
    resolved_count = 0
    for candidate in result.missing_link_candidates:
        if candidate.reason == "origin_task_requires_cross_artifact_resolution":
            origin_id = _suggested_ref(candidate, "task:")
            origin_card = cards_by_id.get(origin_id or "")
            if origin_card is None:
                unresolved.append(candidate)
                continue
            target_cid = _append_card_entity_node(result, origin_card)
            edge_id = f"{candidate.from_candidate_id}_originates_from_{origin_card.id[:8]}"
            if not _edge_exists(result, edge_id):
                result.edges.append(EmittedEdge(
                    candidate_id=edge_id,
                    edge_type="originates_from",
                    from_candidate_id=candidate.from_candidate_id,
                    to_candidate_id=target_cid,
                    confidence=1.0,
                    rule_id=f"originates_from/origin_task_id@{WORKER_VERSION}",
                ))
            resolved_count += 1
            continue

        if candidate.reason == "linked_test_task_requires_cross_artifact_resolution":
            test_task_id = _suggested_ref(candidate, "test_task:")
            test_card = cards_by_id.get(test_task_id or "")
            if test_card is None:
                unresolved.append(candidate)
                continue
            target_cid = _append_card_entity_node(result, test_card)
            edge_id = f"{candidate.from_candidate_id}_covered_by_card_{test_card.id[:8]}"
            if not _edge_exists(result, edge_id):
                result.edges.append(EmittedEdge(
                    candidate_id=edge_id,
                    edge_type="covered_by",
                    from_candidate_id=candidate.from_candidate_id,
                    to_candidate_id=target_cid,
                    confidence=1.0,
                    rule_id=f"covered_by/linked_test_task_id@{WORKER_VERSION}",
                ))

            spec = specs_by_id.get(test_card.spec_id or "")
            for scenario_id in test_card.test_scenario_ids or []:
                if spec is None:
                    continue
                scenario = _scenario_candidate_for_id(spec, str(scenario_id))
                if scenario is None:
                    continue
                spec_cid = _append_spec_entity_node(result, spec)
                scenario_cid, scenario_node = scenario
                if not _node_exists(result, scenario_cid):
                    result.nodes.append(scenario_node)
                scenario_belongs_edge_id = (
                    f"{scenario_cid}_belongs_to_spec_{spec.id[:8]}"
                )
                if not _edge_exists(result, scenario_belongs_edge_id):
                    result.edges.append(EmittedEdge(
                        candidate_id=scenario_belongs_edge_id,
                        edge_type="belongs_to",
                        from_candidate_id=scenario_cid,
                        to_candidate_id=spec_cid,
                        confidence=1.0,
                        rule_id=f"belongs_to/bug_linked_test_scenario@{WORKER_VERSION}",
                    ))
                scenario_edge_id = (
                    f"{candidate.from_candidate_id}_covered_by_ts_"
                    f"{spec.id[:8]}_{str(scenario_id)[:8]}"
                )
                if not _edge_exists(result, scenario_edge_id):
                    result.edges.append(EmittedEdge(
                        candidate_id=scenario_edge_id,
                        edge_type="covered_by",
                        from_candidate_id=candidate.from_candidate_id,
                        to_candidate_id=scenario_cid,
                        confidence=1.0,
                        rule_id=f"covered_by/linked_test_scenario@{WORKER_VERSION}",
                    ))
            resolved_count += 1
            continue

        unresolved.append(candidate)

    result.missing_link_candidates = unresolved
    if resolved_count:
        logger.info(
            "consolidation.missing_links_resolved board=%s resolved=%d unresolved=%d",
            board_id, resolved_count, len(unresolved),
            extra={
                "event": "kg.consolidation.missing_links_resolved",
                "board_id": board_id,
                "resolved_count": resolved_count,
                "unresolved_count": len(unresolved),
            },
        )
    return result


# ---------------------------------------------------------------------------
# Process a single queue entry
# ---------------------------------------------------------------------------


async def _process_queue_entry(
    db: AsyncSession,
    entry: ConsolidationQueue,
) -> bool:
    """Process one queue entry through the primitives pipeline.
    Returns True on success, False on failure."""

    if entry.artifact_type == "story":
        result = await db.execute(
            select(Story).where(Story.id == entry.artifact_id)
        )
    elif entry.artifact_type == "ideation":
        result = await db.execute(
            select(Ideation)
            .options(selectinload(Ideation.story_links))
            .where(Ideation.id == entry.artifact_id)
        )
    elif entry.artifact_type == "refinement":
        result = await db.execute(
            select(Refinement).where(Refinement.id == entry.artifact_id)
        )
    elif entry.artifact_type == "spec":
        result = await db.execute(
            select(Spec)
            .options(selectinload(Spec.architecture_designs))
            .where(Spec.id == entry.artifact_id)
        )
    elif entry.artifact_type == "sprint":
        result = await db.execute(
            select(Sprint).options(selectinload(Sprint.spec)).where(Sprint.id == entry.artifact_id)
        )
    elif entry.artifact_type == "card":
        result = await db.execute(
            select(Card)
            .options(selectinload(Card.architecture_designs))
            .where(Card.id == entry.artifact_id)
        )
    else:
        logger.warning("unknown artifact_type: %s", entry.artifact_type)
        return False

    artifact = result.scalars().first()
    if not artifact:
        logger.warning(
            "%s not found: %s", entry.artifact_type, entry.artifact_id,
        )
        return False

    worker_result = _run_deterministic_worker(entry, artifact)
    worker_result = await _materialize_lineage_endpoint_nodes(
        db,
        entry,
        artifact,
        worker_result,
    )
    worker_result = await _resolve_missing_link_candidates(
        db,
        entry.board_id,
        worker_result,
    )
    node_candidates = [_worker_node_to_candidate(n) for n in worker_result.nodes]
    edge_candidates = [_worker_edge_to_candidate(e) for e in worker_result.edges]
    raw_content = worker_result.raw_content

    logger.info(
        "consolidation.extracted board=%s artifact=%s:%s nodes=%d edges=%d missing=%d",
        entry.board_id, entry.artifact_type, entry.artifact_id,
        len(node_candidates), len(edge_candidates),
        len(worker_result.missing_link_candidates),
    )

    if not node_candidates:
        return True  # nothing to do, but not a failure

    # 1. begin_consolidation (db=None to skip dedup — historical is forced re-processing)
    begin_resp = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=entry.board_id,
            artifact_type=entry.artifact_type,
            artifact_id=entry.artifact_id,
            raw_content=raw_content,
            deterministic_candidates=node_candidates,
        ),
        agent_id=AGENT_ID,
        db=None,
        force_reprocess=True,
    )
    session_id = begin_resp.session_id

    # 2. Add edge candidates
    for edge in edge_candidates:
        await add_edge_candidate(
            AddEdgeCandidateRequest(session_id=session_id, candidate=edge),
            agent_id=AGENT_ID,
        )

    # 3. propose_reconciliation
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=session_id),
        agent_id=AGENT_ID,
        db=None,
        force_reprocess=True,
    )

    # 4. commit + safe lifecycle. The queue row is only acknowledged after
    # graph.lbug survives close/reopen from disk.
    commit_resp = await _commit_consolidation_with_board_graph_lifecycle(
        entry=entry,
        session_id=session_id,
        summary_text=(
            "Historical consolidation of "
            f"{entry.artifact_type} "
            f"'{getattr(artifact, 'title', entry.artifact_id)}'"
        ),
        db=db,
    )

    logger.info(
        "consolidated %s:%s → nodes_added=%d edges_added=%d",
        entry.artifact_type, entry.artifact_id,
        commit_resp.nodes_added, commit_resp.edges_added,
    )
    try:
        debt_result = await mark_canonical_debt_committed_for_artifact(
            db,
            board_id=entry.board_id,
            artifact_type=entry.artifact_type,
            artifact_id=entry.artifact_id,
            actor_id=AGENT_ID,
            evidence_ref=f"kg_session:{session_id}",
        )
        if debt_result["committed_count"]:
            logger.info(
                "canonical_debt.resolved board=%s artifact=%s:%s count=%d",
                entry.board_id, entry.artifact_type, entry.artifact_id,
                debt_result["committed_count"],
            )
    except Exception:
        logger.exception(
            "canonical_debt.resolve_failed board=%s artifact=%s:%s",
            entry.board_id, entry.artifact_type, entry.artifact_id,
        )

    # R7 IMP2: keep the canonical Learning partition-integrity ledger current —
    # open CanonicalDebt for historical violations (canonical bug-derived
    # Learning without a canonical Bug) and close debt whose bug evidence is now
    # canonical (canonical-only evidence pre-filter). Reuses canonical_debt_service;
    # never cognitive pending/DLQ. Best effort — must never fail a good commit.
    try:
        from okto_pulse.core.kg.canonical_learning_partition import (
            run_canonical_learning_partition_maintenance,
        )

        await run_canonical_learning_partition_maintenance(
            db, board_id=entry.board_id, actor_id=AGENT_ID
        )
    except Exception:
        logger.exception(
            "kg.clp.maintenance_failed board=%s artifact=%s:%s",
            entry.board_id, entry.artifact_type, entry.artifact_id,
        )
    return True


async def _classify_queue_entry_source_for_debt(
    db: AsyncSession,
    entry: ConsolidationQueue,
):
    """Return the current source maturity for queue-failure accounting.

    CanonicalDebt is specifically canonical debt. A failed working-graph
    materialization remains operational debt in the queue/DLQ, but must not
    inflate the canonical-debt counters used by KG Health.
    """

    if entry.artifact_type == "card":
        card = await db.get(Card, entry.artifact_id)
        if card is None:
            return None
        card_type = (
            getattr(card.card_type, "value", card.card_type)
            if card.card_type
            else "normal"
        )
        return classify_source_for_kg(
            artifact_type=_card_source_artifact_type(card_type),
            artifact_status=getattr(
                getattr(card, "status", None),
                "value",
                getattr(card, "status", None),
            ),
            content_hash="consolidation-failure",
            has_minimal_evidence=_card_has_minimal_evidence(card),
        )

    model_by_type = {
        "story": Story,
        "ideation": Ideation,
        "refinement": Refinement,
        "spec": Spec,
        "sprint": Sprint,
    }
    model = model_by_type.get(entry.artifact_type)
    if model is None:
        return None
    artifact = await db.get(model, entry.artifact_id)
    if artifact is None:
        return None
    return classify_source_for_kg(
        artifact_type=entry.artifact_type,
        artifact_status=getattr(
            getattr(artifact, "status", None),
            "value",
            getattr(artifact, "status", None),
        ),
        content_hash="consolidation-failure",
    )


# ---------------------------------------------------------------------------
# Worker class
# ---------------------------------------------------------------------------


class ConsolidationWorker:
    """Async background worker that drains consolidation_queue through the
    deterministic Layer 1 pipeline.

    Trigger model (Fase 4): primarily event-driven via an internal
    `asyncio.Event` that enqueue sites signal on. The `heartbeat_seconds`
    sleep is a safety-net so the worker still wakes up periodically even
    when the signal is dropped (e.g. singleton was restarted mid-flight).
    """

    # Entries claimed longer than this (minutes) are considered stuck.
    STALE_CLAIM_MINUTES: int = 30

    def __init__(
        self,
        session_factory,
        heartbeat_seconds: int = 30,
        batch_size: int = 5,
        stale_claim_minutes: int | None = None,
    ):
        self.session_factory = session_factory
        self.heartbeat_seconds = heartbeat_seconds
        self.batch_size = batch_size
        self._stale_claim_minutes = stale_claim_minutes or self.STALE_CLAIM_MINUTES
        self._task: asyncio.Task | None = None
        self._recovery_task: asyncio.Task | None = None
        self._running = False
        # Lazily created in start() so the event binds to the running loop.
        self._wake_event: asyncio.Event | None = None
        # FR5/FR6 (spec R2c): per-board DLQ auto-drain state.
        # _dlq_drain_last_run: board_id -> datetime of last drain attempt.
        # _dlq_drain_last_requeued: board_id -> requeued_count of last run.
        self._dlq_drain_last_run: dict[str, datetime] = {}
        self._dlq_drain_last_requeued: dict[str, int] = {}

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def snapshot_pool(self) -> dict[str, int]:
        """Return a {active, idle, draining} snapshot of the worker pool.

        Spec bdcda842 (TR18 + FR9): consumed by /api/v1/kg/queue/health.
        The current single-task implementation reports the configured
        ``kg_queue_max_concurrent_workers`` as the pool size (active when
        the task is alive, draining=0 in steady state). The dynamic
        worker-pool refactor lives in IMPL-4 follow-ups.
        """
        from okto_pulse.core.infra.config import get_settings

        try:
            max_workers = int(get_settings().kg_queue_max_concurrent_workers)
        except Exception:
            max_workers = 1
        active = max_workers if self.is_running else 0
        return {"active": active, "idle": 0, "draining": 0}

    def get_dlq_drain_stats(self, board_id: str) -> dict:
        """Return DLQ auto-drain stats for ``board_id`` (FR6, spec R2c).

        Returns ``{last_run_at: ISO-string|None, requeued_count: int}``.
        Both values come from in-process tracking only — process restart
        resets them. The health endpoint uses this to populate the additive
        ``dlq_auto_drain_*`` fields without touching storage.
        """
        last_run = self._dlq_drain_last_run.get(board_id)
        return {
            "last_run_at": last_run.isoformat() if last_run is not None else None,
            "requeued_count": self._dlq_drain_last_requeued.get(board_id, 0),
        }

    def signal_new_work(self) -> None:
        """Wake the run-loop now. Safe to call from any coroutine in the
        same event loop — used by enqueue sites to get near-instant
        processing without waiting for the heartbeat."""
        evt = self._wake_event
        if evt is not None:
            try:
                evt.set()
            except RuntimeError:
                # Event was bound to a different loop (tests / forked
                # processes) — ignore, heartbeat will pick the work up.
                pass

    async def _reclaim_stale_claims(self) -> int:
        """Re-pending queue entries whose claim timeout has elapsed.

        Spec bdcda842 (BR Recovery scan + TR6): the new contract uses the
        per-row ``claim_timeout_at`` field set at claim time
        (now + kg_queue_claim_timeout_s). When the worker that holds the
        claim crashes or is killed, ``claim_timeout_at`` eventually elapses
        and the next recovery scan picks the row up.

        Falls back to the legacy ``stale_claim_minutes`` cutoff for rows
        claimed by an older binary that didn't populate ``claim_timeout_at``
        (so partial migrations don't strand work). Returns the count of
        rows reset to ``pending``.
        """
        now = datetime.now(timezone.utc)
        legacy_cutoff = now - timedelta(minutes=self._stale_claim_minutes)
        async with self.session_factory() as db:
            result = await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.status == "claimed",
                    (
                        (ConsolidationQueue.claim_timeout_at.is_not(None))
                        & (ConsolidationQueue.claim_timeout_at < now)
                    )
                    | (
                        ConsolidationQueue.claim_timeout_at.is_(None)
                        & ConsolidationQueue.claimed_at.is_not(None)
                        & (ConsolidationQueue.claimed_at < legacy_cutoff)
                    ),
                )
            )
            stale = list(result.scalars().all())
            if not stale:
                return 0
            for entry in stale:
                entry.status = "pending"
                entry.claimed_at = None
                entry.claim_timeout_at = None
                entry.worker_id = None
                entry.claimed_by_session_id = None
            await db.commit()
        logger.info(
            "kg.consolidation_worker.recovered count=%d",
            len(stale),
            extra={
                "event": "kg.queue.recovered",
                "count": len(stale),
            },
        )
        return len(stale)

    async def _recovery_scan_loop(self) -> None:
        """Periodic background scan that re-pendings orphaned claims.

        Spec bdcda842 (TR6): runs as an asyncio.Task on the FastAPI
        lifespan; interval is ``settings.kg_queue_recovery_scan_interval_s``
        (default 60s). The loop reads the setting on every iteration so
        operators can lower the interval without restarting (mirrors the
        worker pool hot-reload contract).
        """
        from okto_pulse.core.infra.config import get_settings as _gs
        try:
            while self._running:
                try:
                    await self._reclaim_stale_claims()
                except Exception as exc:
                    logger.error(
                        "kg.consolidation_worker.recovery_scan_failed: %s", exc,
                        exc_info=True,
                    )
                interval_s = _gs().kg_queue_recovery_scan_interval_s
                await asyncio.sleep(max(1, int(interval_s)))
        except asyncio.CancelledError:
            pass

    async def start(self) -> None:
        if self.is_running:
            return
        # Reclaim any entries left in 'claimed' from a previous crash.
        await self._reclaim_stale_claims()
        self._running = True
        self._wake_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._run_loop(), name="kg.consolidation_worker"
        )
        # Spec bdcda842 (TR6): periodic recovery scan as a sibling task.
        self._recovery_task = asyncio.create_task(
            self._recovery_scan_loop(), name="kg.consolidation_recovery_scan"
        )
        logger.info(
            "kg.consolidation_worker.started heartbeat=%ds",
            self.heartbeat_seconds,
        )

    async def stop(self, timeout: float = 10.0) -> None:
        if not self.is_running:
            self._running = False
            return
        self._running = False
        assert self._task is not None
        self._task.cancel()
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        recovery_task = getattr(self, "_recovery_task", None)
        if recovery_task is not None and not recovery_task.done():
            recovery_task.cancel()
            try:
                await asyncio.wait_for(recovery_task, timeout=timeout)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        self._task = None
        self._recovery_task = None
        self._wake_event = None
        logger.info("kg.consolidation_worker.stopped")

    async def process_batch(self) -> int:
        """Process up to batch_size pending entries. Returns count processed.

        Spec bdcda842 (Sprint 2):
            * **Claim board-aware** — prefer items whose board_id is NOT
              already claimed by another worker (so distinct boards process
              in parallel; same-board items still serialise on the per-board
              Kùzu file lock via commit_coordinator).
            * **Backoff-aware claim** — skip items where ``next_retry_at``
              hasn't elapsed yet (BR Dead-letter / exp backoff).
            * **DELETE-on-ack** — successful processing removes the row from
              ConsolidationQueue (at-least-once semantics: row stays until
              the consolidate+commit pipeline confirmed).
            * **Failure path** — increment ``attempts``, persist
              ``last_error``, schedule ``next_retry_at = now + min(2^N, 300)s``
              and put the row back to ``pending`` for the next claim. The
              dead-letter routing (after ``kg_queue_max_attempts``) is
              entered through the same path and lives in IMPL-3 wiring.

        Adaptive batch sizing keeps the catch-up behaviour from the prior
        implementation; each entry processed in its own session to keep
        SQLite transactions short.
        """
        from okto_pulse.core.infra.config import get_settings

        processed = 0
        settings = get_settings()
        claim_timeout_s = settings.kg_queue_claim_timeout_s

        # Step 1: Claim entries (fast DB update, single session).
        async with self.session_factory() as db:
            depth_result = await db.execute(
                select(func.count()).where(
                    ConsolidationQueue.status == "pending",
                )
            )
            pending_depth = depth_result.scalar_one()

            if pending_depth > 200:
                effective_batch = 50
            elif pending_depth > 100:
                effective_batch = 20
            elif pending_depth > 50:
                effective_batch = 10
            else:
                effective_batch = self.batch_size

            if effective_batch != self.batch_size:
                logger.info(
                    "consolidation.adaptive_batch depth=%d batch_size=%d",
                    pending_depth, effective_batch,
                )

            now = datetime.now(timezone.utc)

            # Subquery: which boards already have an in-flight claimed item?
            # Items on those boards still get claimed at the end (fallback
            # to FIFO) so progress isn't blocked when only one board has
            # work — but we prefer distinct boards first.
            claimed_boards_subq = (
                select(ConsolidationQueue.board_id)
                .where(ConsolidationQueue.status == "claimed")
                .scalar_subquery()
            )

            ready_filter = (
                (ConsolidationQueue.next_retry_at.is_(None))
                | (ConsolidationQueue.next_retry_at <= now)
            )

            board_aware_q = (
                select(ConsolidationQueue)
                .where(
                    ConsolidationQueue.status == "pending",
                    ready_filter,
                    ConsolidationQueue.board_id.notin_(claimed_boards_subq),
                )
                .order_by(
                    ConsolidationQueue.priority.asc(),
                    ConsolidationQueue.triggered_at.asc(),
                )
                .limit(effective_batch)
            )
            result = await db.execute(board_aware_q)
            entries = list(result.scalars().all())

            if len(entries) < effective_batch:
                # Top up with FIFO from the remaining boards (boards that
                # already had a claim but still have backlog). Ensures
                # progress when there is exactly one board doing work.
                already = {e.id for e in entries}
                fallback_q = (
                    select(ConsolidationQueue)
                    .where(
                        ConsolidationQueue.status == "pending",
                        ready_filter,
                    )
                    .order_by(
                        ConsolidationQueue.priority.asc(),
                        ConsolidationQueue.triggered_at.asc(),
                    )
                    .limit(effective_batch)
                )
                fallback = list((await db.execute(fallback_q)).scalars().all())
                for fb in fallback:
                    if fb.id in already:
                        continue
                    entries.append(fb)
                    if len(entries) >= effective_batch:
                        break

            claim_timeout_at = now + timedelta(seconds=claim_timeout_s)
            for entry in entries:
                entry.status = "claimed"
                entry.claimed_at = now
                entry.claim_timeout_at = claim_timeout_at
                worker_id = f"worker_{uuid.uuid4().hex[:8]}"
                entry.worker_id = worker_id
                # Keep claimed_by_session_id populated for backward-compat
                # with cognitive-session inspectors that still read it.
                entry.claimed_by_session_id = worker_id
            await db.commit()

            # Spec bdcda842 (TR13): claims_per_min sliding window for
            # /api/v1/kg/queue/health. Recorded after a successful claim
            # commit so retries don't double-count.
            if entries:
                from okto_pulse.core.services.queue_health_service import (
                    record_claim,
                )
                for _ in entries:
                    record_claim(now=now)

        # Step 2: Process each entry with its own session (short-lived tx).
        max_attempts = settings.kg_queue_max_attempts
        for entry in entries:
            # Cede o event loop entre entries — flagrado por py-spy
            # (2026-06-10) ocupando o loop em rajadas durante batches
            # grandes, deixando a UI sem resposta (nem static files saíam).
            await asyncio.sleep(0)
            try:
                async with self.session_factory() as db:
                    success = await _process_queue_entry_serialized(db, entry)
                    fresh = await db.get(ConsolidationQueue, entry.id)
                    if fresh is None:
                        # Row was already removed (e.g. recovery scan +
                        # another worker raced past us — at-least-once
                        # tolerates that).
                        await db.commit()
                        if success:
                            processed += 1
                        continue

                    if success:
                        # DELETE-on-ack: row only disappears once the
                        # commit + recompute completed. Any crash before
                        # this point keeps the row claimed; the recovery
                        # scan re-pendings it after claim_timeout_at.
                        await db.delete(fresh)
                    else:
                        await self._mark_failed(
                            db, fresh,
                            error_text=fresh.last_error or "processing returned False",
                            max_attempts=max_attempts,
                        )
                    await db.commit()
                if success:
                    processed += 1
            except Exception as exc:
                logger.error(
                    "consolidation failed for %s:%s: %s",
                    entry.artifact_type, entry.artifact_id, exc,
                    exc_info=True,
                )
                try:
                    async with self.session_factory() as db:
                        fresh = await db.get(ConsolidationQueue, entry.id)
                        if fresh:
                            await self._mark_failed(
                                db, fresh,
                                error_text=f"{type(exc).__name__}: {str(exc)[:480]}",
                                max_attempts=max_attempts,
                            )
                        await db.commit()
                except Exception:
                    pass

        return processed

    async def _mark_failed(
        self,
        db: AsyncSession,
        entry: ConsolidationQueue,
        *,
        error_text: str,
        max_attempts: int,
    ) -> None:
        """Common failure handler: increment attempts, schedule exp backoff,
        re-pending the row. When ``attempts >= max_attempts`` the row is
        instead routed to ``ConsolidationDeadLetter`` (IMPL-3 wiring) and
        deleted from the queue. Caller is responsible for the commit.

        FR3 (spec R2c): when the entry is routed to the dead-letter queue,
        a FailureEvent with ``event_kind="kg.commit.failed"`` is recorded
        in the collector ring-buffer so the MemoryPressureCorrelator
        receives a real commit-failure signal.  Non-blocking/non-raising.

        FR6 (spec eaf185c9 / card 81a96a49): a legacy board missing the
        graph_layer/maturity_status schema raises ``Cannot find property
        graph_layer for n``. Before that raw string becomes the sole DLQ
        diagnostic we try the idempotent schema migration+backfill. If it
        actually repairs the schema we re-pending the entry for an immediate
        retry instead of counting it toward the dead-letter threshold; if it
        cannot, we replace the raw error with a structured, actionable
        diagnostic (or_1f52d4fd) so the dead-letter row names the operational
        action rather than the opaque binder error.
        """
        if is_graph_layer_schema_error(error_text):
            remediation = ensure_graph_layer_schema(
                entry.board_id, raw_error=error_text
            )
            if remediation.recovered:
                # Schema repaired in place — re-pending for an immediate retry
                # rather than charging this attempt against the DLQ threshold.
                entry.last_error = None
                entry.status = "pending"
                entry.next_retry_at = datetime.now(timezone.utc)
                entry.claim_timeout_at = None
                entry.worker_id = None
                entry.claimed_at = None
                entry.claimed_by_session_id = None
                logger.info(
                    "consolidation.schema_layer_recovered artifact=%s:%s "
                    "board=%s columns_added=%s",
                    entry.artifact_type, entry.artifact_id, entry.board_id,
                    remediation.columns_added,
                )
                return
            if remediation.needs_structured_error and remediation.structured_message:
                # Could not migrate — make the DLQ diagnostic actionable so the
                # raw binder error is never the only thing operators see.
                error_text = remediation.structured_message

        correlation_id = uuid.uuid4().hex
        try:
            classification = await _classify_queue_entry_source_for_debt(db, entry)
            default_to_canonical_debt = (
                classification is None
                and entry.artifact_type in {"spec", "refinement"}
            )
            is_canonical_failure = (
                default_to_canonical_debt
                or (
                    classification is not None
                    and classification.graph_layer == GRAPH_LAYER_CANONICAL
                    and classification.maturity_status == MATURITY_CANONICAL_ELIGIBLE
                )
            )
            if is_canonical_failure:
                debt_hash = hashlib.sha256(
                    "|".join([
                        entry.board_id,
                        entry.artifact_type,
                        entry.artifact_id,
                        entry.triggered_at.isoformat() if entry.triggered_at else "",
                    ]).encode("utf-8")
                ).hexdigest()
                await upsert_canonical_debt(
                    db,
                    board_id=entry.board_id,
                    artifact_type=entry.artifact_type,
                    artifact_id=entry.artifact_id,
                    source_ref=f"{entry.artifact_type}:{entry.artifact_id}",
                    content_hash=debt_hash,
                    target_status="canonical_consolidation",
                    canonical_state="failed",
                    failure_reason="consolidation_failed",
                    last_error=error_text,
                    owner_agent_id=entry.worker_id or AGENT_ID,
                    correlation_id=correlation_id,
                    queue_ref=entry.id,
                    graph_layer=(
                        classification.graph_layer
                        if classification is not None
                        else GRAPH_LAYER_CANONICAL
                    ),
                    maturity_status=(
                        classification.maturity_status
                        if classification is not None
                        else MATURITY_CANONICAL_ELIGIBLE
                    ),
                )
        except Exception as debt_exc:
            logger.error(
                "canonical_debt.persist_failed board=%s artifact=%s:%s err=%s",
                entry.board_id, entry.artifact_type, entry.artifact_id,
                debt_exc,
            )

        entry.attempts = (entry.attempts or 0) + 1
        entry.last_error = error_text
        if entry.attempts >= max_attempts:
            await route_to_dead_letter(db, entry, error_text=error_text)
            # FR3: record commit failure for the memory-pressure correlator.
            try:
                record_failure(
                    entry.board_id,
                    FailureEvent(
                        timestamp=datetime.now(timezone.utc),
                        event_kind="kg.commit.failed",
                        graph_type="board",
                        correlation_id=correlation_id,
                    ),
                )
            except Exception:
                pass
            return
        backoff_s = min(2 ** entry.attempts, 300)
        entry.status = "pending"
        entry.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_s)
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_at = None
        entry.claimed_by_session_id = None
        logger.info(
            "consolidation.attempt_failed artifact=%s:%s attempts=%d "
            "next_retry_in=%ds",
            entry.artifact_type, entry.artifact_id, entry.attempts, backoff_s,
        )

    async def _run_dlq_auto_drain(self) -> None:
        """FR5/FR6 (spec R2c): opt-in automatic DLQ reprocess on each heartbeat.

        For every board that has ``dlq_auto_drain_enabled=True`` in its
        ``Board.settings`` JSON column AND has dead-letter rows pending, we
        call ``reprocess_dead_letter_rows`` at most once per
        ``kg_queue_dlq_auto_drain_backoff_s`` seconds (in-process per-board
        cooldown dictionary, not persisted).

        Poison-pill guard: DLQ rows whose ``attempts`` counter has reached
        ``kg_queue_dlq_auto_drain_max_requeue_attempts`` are permanently
        deleted and a WARN log is emitted so operators know they have an
        artifact that cannot be consolidated.

        The settings are re-read from the DB on every heartbeat so operators
        can enable/disable the feature per board at runtime without a restart.
        """
        from okto_pulse.core.infra.config import get_settings
        from okto_pulse.core.models.db import Board, ConsolidationDeadLetter
        from okto_pulse.core.services.dead_letter_inspector_service import (
            reprocess_dead_letter_rows,
        )

        try:
            settings = get_settings()
            backoff_s: int = settings.kg_queue_dlq_auto_drain_backoff_s
            max_attempts: int = settings.kg_queue_dlq_auto_drain_max_requeue_attempts
        except Exception:
            return  # defensive: don't break the heartbeat on config failure

        now = datetime.now(timezone.utc)

        try:
            async with self.session_factory() as db:
                # Load all boards that have dlq_auto_drain_enabled=True.
                board_result = await db.execute(select(Board))
                boards = list(board_result.scalars().all())

            enabled_boards = [
                b for b in boards
                if isinstance(b.settings, dict)
                and b.settings.get("dlq_auto_drain_enabled")
            ]

            for board in enabled_boards:
                board_id = board.id

                # Backoff: skip if we ran recently for this board
                last_run = self._dlq_drain_last_run.get(board_id)
                if last_run is not None:
                    elapsed = (now - last_run).total_seconds()
                    if elapsed < backoff_s:
                        continue  # AC11: still within backoff window

                # Check if the board actually has DLQ rows
                async with self.session_factory() as db:
                    dlq_count = await db.scalar(
                        select(func.count()).where(
                            ConsolidationDeadLetter.board_id == board_id
                        )
                    ) or 0

                if dlq_count == 0:
                    continue

                # Poison-pill exclusion: remove rows at or beyond max attempts
                # before passing them to the requeue path (so they don't just
                # cycle back to DLQ again immediately).
                skipped_poison: list[str] = []
                async with self.session_factory() as db:
                    poison_result = await db.execute(
                        select(ConsolidationDeadLetter).where(
                            ConsolidationDeadLetter.board_id == board_id,
                            ConsolidationDeadLetter.attempts >= max_attempts,
                        )
                    )
                    poison_rows = list(poison_result.scalars().all())
                    for row in poison_rows:
                        skipped_poison.append(row.id)
                        logger.warning(
                            "kg.dlq.auto_drain.poison_pill_excluded "
                            "board_id=%s dlq_id=%s attempts=%d max=%d",
                            board_id, row.id, row.attempts, max_attempts,
                            extra={
                                "event": "kg.dlq.auto_drain.poison_pill_excluded",
                                "board_id": board_id,
                                "dlq_id": row.id,
                                "attempts": row.attempts,
                                "max_requeue_attempts": max_attempts,
                            },
                        )
                        await db.delete(row)
                    if poison_rows:
                        await db.commit()

                # Reprocess the remaining (non-poison) rows
                async with self.session_factory() as db:
                    result = await reprocess_dead_letter_rows(db, board_id, limit=50)
                    await db.commit()

                requeued_count: int = result.get("requeued_count", 0)
                already_queued_count: int = result.get("already_queued_count", 0)

                self._dlq_drain_last_run[board_id] = now
                self._dlq_drain_last_requeued[board_id] = requeued_count

                logger.info(
                    "kg.dlq.auto_drain board_id=%s requeued=%d already_queued=%d skipped=%d",
                    board_id, requeued_count, already_queued_count, len(skipped_poison),
                    extra={
                        "event": "kg.dlq.auto_drain",
                        "board_id": board_id,
                        "requeued": requeued_count,
                        "already_queued": already_queued_count,
                        "skipped": len(skipped_poison),
                    },
                )

        except Exception as exc:
            logger.warning(
                "kg.dlq.auto_drain.failed: %s", exc, exc_info=True,
            )

    async def _run_loop(self) -> None:
        try:
            while self._running:
                try:
                    processed = await self.process_batch()
                    if processed > 0:
                        logger.info(
                            "kg.consolidation_worker.batch processed=%d", processed,
                        )
                        # Keep draining while there is real progress. The
                        # rebuild path waits synchronously for this queue to
                        # reach zero before promoting a generation; sleeping
                        # the full heartbeat between successful batches turns
                        # large deterministic rebuilds into artificial
                        # timeout failures even though the worker is healthy.
                        await asyncio.sleep(0)
                        continue
                except Exception as exc:
                    logger.error(
                        "kg.consolidation_worker.batch_failed: %s", exc, exc_info=True,
                    )

                # Wait for either a wake signal or the heartbeat tick —
                # whichever comes first. Clearing the event after wait
                # keeps signals coalesced (many signals → one batch).
                evt = self._wake_event
                if evt is None:
                    # Defensive: start() always creates the event, but if
                    # stop() is racing we just fall back to a short sleep.
                    await asyncio.sleep(self.heartbeat_seconds)
                    continue

                try:
                    await asyncio.wait_for(evt.wait(), timeout=self.heartbeat_seconds)
                except asyncio.TimeoutError:
                    pass
                evt.clear()
                # FR5 (spec R2c): opt-in DLQ auto-drain on every heartbeat.
                # Runs after the wake/timeout so it does not delay queue
                # draining when there is real consolidation work pending.
                await self._run_dlq_auto_drain()
        except asyncio.CancelledError:
            pass


_singleton: ConsolidationWorker | None = None


def get_consolidation_worker(session_factory=None) -> ConsolidationWorker:
    """Return the process-wide consolidation worker."""
    global _singleton
    if _singleton is None:
        if session_factory is None:
            from okto_pulse.core.infra.database import get_session_factory
            session_factory = get_session_factory()
        _singleton = ConsolidationWorker(session_factory=session_factory)
    return _singleton


def reset_consolidation_worker_for_tests() -> None:
    global _singleton
    _singleton = None


def signal_consolidation_worker() -> None:
    """Module-level helper: wake the process-wide worker if one is running.
    Enqueue sites call this right after committing new rows so the worker
    picks them up without waiting for the heartbeat."""
    if _singleton is not None and _singleton.is_running:
        _singleton.signal_new_work()
