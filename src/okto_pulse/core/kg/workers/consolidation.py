"""Background worker that drains the consolidation_queue (spec c48a5c33).

For each pending queue entry the worker:
    1. Loads the artifact (Spec/Sprint/Card) from the DB.
    2. Runs the pure `DeterministicWorker` (Layer 1) to extract every
       node + edge candidate that can be derived from structured fields,
       with full v0.2.0 provenance metadata (layer/rule_id/created_by).
    3. Drives the primitives pipeline: begin → propose_reconciliation →
       commit. The session uses `agent_id="system:historical_consolidation"`
       so the layer-ownership BR allows deterministic edges through.
    4. Marks the queue entry as `done` (or `failed`).

The cognitive agent picks up `missing_link_candidates` later and proposes
the residual semantic edges (capped at confidence 0.85 per BR `Cognitive
Fallback Confidence Cap`).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from okto_pulse.core.models.db import (
    ConsolidationQueue,
    Spec,
    Sprint,
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
from okto_pulse.core.kg.workers.deterministic_worker import (
    DeterministicWorker,
    EmittedEdge,
    EmittedNode,
    WorkerResult,
)

logger = logging.getLogger("okto_pulse.kg.consolidation_worker")

AGENT_ID = "system:historical_consolidation"


# ---------------------------------------------------------------------------
# Adapter: SQLAlchemy artifact → DeterministicWorker dict shape
# ---------------------------------------------------------------------------


def _spec_to_dict(spec: Spec) -> dict:
    """Serialise a Spec row into the dict shape DeterministicWorker expects.
    Mirrors the JSON emitted by the Spec API routes so unit tests run under
    the same contract as production callers."""
    return {
        "id": spec.id,
        "title": spec.title,
        "description": spec.description,
        "context": spec.context,
        "functional_requirements": spec.functional_requirements or [],
        "technical_requirements": spec.technical_requirements or [],
        "acceptance_criteria": spec.acceptance_criteria or [],
        "business_rules": spec.business_rules or [],
        "test_scenarios": spec.test_scenarios or [],
        "api_contracts": spec.api_contracts or [],
    }


def _sprint_to_dict(sprint: Sprint) -> dict:
    return {
        "id": sprint.id,
        "title": sprint.title,
        "description": sprint.description,
        "objective": sprint.objective,
        "expected_outcome": sprint.expected_outcome,
        "spec_id": sprint.spec_id,
    }


def _card_to_dict(card) -> dict:
    priority = getattr(card, "priority", None)
    return {
        "id": card.id,
        "title": card.title,
        "description": card.description,
        "card_type": getattr(card.card_type, "value", card.card_type) if getattr(card, "card_type", None) else "normal",
        "spec_id": card.spec_id,
        "sprint_id": card.sprint_id,
        "origin_task_id": getattr(card, "origin_task_id", None),
        "priority": getattr(priority, "value", priority) if priority is not None else None,
    }


def _worker_node_to_candidate(node: EmittedNode) -> NodeCandidate:
    return NodeCandidate(
        candidate_id=node.candidate_id,
        node_type=KGNodeType(node.node_type),
        title=node.title,
        content=node.content,
        context=node.context or None,
        source_artifact_ref=node.source_artifact_ref,
        source_confidence=node.source_confidence,
        priority_boost=node.priority_boost,
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
    if entry.artifact_type == "spec":
        return worker.process_spec(_spec_to_dict(artifact))
    if entry.artifact_type == "sprint":
        return worker.process_sprint(_sprint_to_dict(artifact))
    if entry.artifact_type == "card":
        return worker.process_card(_card_to_dict(artifact))
    raise ValueError(f"unknown artifact_type: {entry.artifact_type}")


# ---------------------------------------------------------------------------
# Process a single queue entry
# ---------------------------------------------------------------------------


async def _process_queue_entry(
    db: AsyncSession,
    entry: ConsolidationQueue,
) -> bool:
    """Process one queue entry through the primitives pipeline.
    Returns True on success, False on failure."""

    if entry.artifact_type == "spec":
        result = await db.execute(select(Spec).where(Spec.id == entry.artifact_id))
    elif entry.artifact_type == "sprint":
        result = await db.execute(
            select(Sprint).options(selectinload(Sprint.spec)).where(Sprint.id == entry.artifact_id)
        )
    elif entry.artifact_type == "card":
        from okto_pulse.core.models.db import Card
        result = await db.execute(select(Card).where(Card.id == entry.artifact_id))
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
    )

    # 4. commit
    commit_resp = await commit_consolidation(
        CommitConsolidationRequest(
            session_id=session_id,
            summary_text=f"Historical consolidation of {entry.artifact_type} '{getattr(artifact, 'title', entry.artifact_id)}'",
        ),
        agent_id=AGENT_ID,
        db=db,
    )

    logger.info(
        "consolidated %s:%s → nodes_added=%d edges_added=%d",
        entry.artifact_type, entry.artifact_id,
        commit_resp.nodes_added, commit_resp.edges_added,
    )
    return True


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

    def __init__(
        self,
        session_factory,
        heartbeat_seconds: int = 30,
        batch_size: int = 5,
    ):
        self.session_factory = session_factory
        self.heartbeat_seconds = heartbeat_seconds
        self.batch_size = batch_size
        self._task: asyncio.Task | None = None
        self._running = False
        # Lazily created in start() so the event binds to the running loop.
        self._wake_event: asyncio.Event | None = None

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

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

    async def start(self) -> None:
        if self.is_running:
            return
        self._running = True
        self._wake_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._run_loop(), name="kg.consolidation_worker"
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
        self._task = None
        self._wake_event = None
        logger.info("kg.consolidation_worker.stopped")

    async def process_batch(self) -> int:
        """Process up to batch_size pending entries. Returns count processed."""
        processed = 0
        async with self.session_factory() as db:
            result = await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.status == "pending",
                ).order_by(
                    ConsolidationQueue.priority.asc(),
                    ConsolidationQueue.triggered_at.asc(),
                ).limit(self.batch_size)
            )
            entries = list(result.scalars().all())

            for entry in entries:
                entry.status = "claimed"
                entry.claimed_at = datetime.now(timezone.utc)
                entry.claimed_by_session_id = f"worker_{uuid.uuid4().hex[:8]}"
                await db.commit()

                try:
                    success = await _process_queue_entry(db, entry)
                    entry.status = "done" if success else "failed"
                    if not success and not entry.last_error:
                        # _process_queue_entry returned False without raising.
                        # Record a generic marker so ops can distinguish
                        # "returned False" from "raised exception".
                        entry.last_error = "processing returned False"
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
                        entry.status = "failed"
                        # Ideação 0605edb2: persist last_error so the
                        # Pending Queue view and /retry flow have a
                        # diagnosable signal instead of last_error=None.
                        entry.last_error = f"{type(exc).__name__}: {str(exc)[:480]}"
                        await db.commit()
                    except Exception:
                        await db.rollback()

        return processed

    async def _run_loop(self) -> None:
        try:
            while self._running:
                try:
                    processed = await self.process_batch()
                    if processed > 0:
                        logger.info(
                            "kg.consolidation_worker.batch processed=%d", processed,
                        )
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
