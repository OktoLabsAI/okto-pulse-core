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
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
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
    severity = getattr(card, "severity", None)
    return {
        "id": card.id,
        "title": card.title,
        "description": card.description,
        "card_type": getattr(card.card_type, "value", card.card_type) if getattr(card, "card_type", None) else "normal",
        "spec_id": card.spec_id,
        "sprint_id": card.sprint_id,
        "origin_task_id": getattr(card, "origin_task_id", None),
        "priority": getattr(priority, "value", priority) if priority is not None else None,
        "severity": getattr(severity, "value", severity) if severity is not None else None,
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
    if entry.artifact_type == "refinement":
        # Spec eaf78891 (Ideação #2): graceful no-op for refinement.
        # Refinement extraction is a follow-up; for now we accept the entry
        # without crashing so RefinementSemanticChanged events flow cleanly.
        return WorkerResult()
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
    elif entry.artifact_type == "refinement":
        # Spec eaf78891 (Ideação #2): refinement is accepted as a no-op.
        # Logged + treated as success so the queue entry is cleared without
        # touching extractors or the KG.
        logger.info(
            "consolidation.refinement.noop board=%s artifact_id=%s source=%s",
            entry.board_id, entry.artifact_id, entry.source,
        )
        return True
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
            try:
                async with self.session_factory() as db:
                    success = await _process_queue_entry(db, entry)
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
        deleted from the queue. Caller is responsible for the commit."""
        from okto_pulse.core.kg.workers.dead_letter import route_to_dead_letter

        entry.attempts = (entry.attempts or 0) + 1
        entry.last_error = error_text
        if entry.attempts >= max_attempts:
            await route_to_dead_letter(db, entry, error_text=error_text)
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
