"""Production rebuild step adapter for KG-02 (bug b4c6920c).

Wires the KG-02 rebuild service to the existing consolidation pipeline
without reimplementing materialization. Strategy is **enqueue-then-wake**:

1. The rebuild service holds the KG-01 admin single-writer lock.
2. This adapter receives the source set already enumerated by
   :class:`BoardSourceStore` (KG-02.2 ``RebuildSourceEnumerator`` passes
   it forward through ``sources_payload``).
3. For each source row we UPSERT into ``ConsolidationQueue`` with the
   same dedup semantics as ``ConsolidationEnqueuer`` (insert if new,
   reset terminal rows to pending, leave pending/claimed alone). Rows use
   high priority because explicit recovery must not sit behind unrelated
   corrupt-board backlog.
4. We signal the consolidation worker so it picks up the new rows
   without waiting for its heartbeat.
5. The structural_hash / source_hash come from KG-02.5
   ``DeterministicStructuralRebuilder`` over the same source set so the
   rebuild report carries a deterministic receipt.

Trade-off documented:
* The adapter returns ``ok=True`` as soon as the rows are enqueued.
  Actual KG mutation happens asynchronously inside the consolidation
  worker (which has its own per-board commit lock that nests safely
  inside the KG-01 admin lock — the existing worker already serialises
  board-by-board). For E2E we expose a ``drain_until_idle`` helper that
  can be wired by callers that want synchronous wait-until-done
  semantics.
* The adapter uses stdlib ``sqlite3`` for the UPSERT because the KG-01
  ``rebuild_step_adapter`` callable is synchronous. The same SQLite
  file is shared with the async SQLAlchemy engine; readers and writers
  are serialised by SQLite's own file-level locks.
"""

from __future__ import annotations

import logging
import sqlite3
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger("okto_pulse.kg.board_rebuild_adapter")


# Source artifact types that get deterministic rebuild ingestion.
# Refinement and decision are intentionally excluded here: they are
# semantic-only cognitive debt. Decision nodes are materialized through the
# owning spec payload. task/test/bug are card-derived source types and are
# mapped to the worker's legacy ``card`` artifact_type in ConsolidationQueue.
_DETERMINISTIC_SOURCE_ARTIFACT_TYPES: frozenset[str] = frozenset({
    "spec", "task", "test", "bug", "card",
})
_CARD_SOURCE_ARTIFACT_TYPES: frozenset[str] = frozenset({
    "task", "test", "bug", "card",
})


def _queue_artifact_type(source_artifact_type: str) -> str:
    return "card" if source_artifact_type in _CARD_SOURCE_ARTIFACT_TYPES else source_artifact_type


@dataclass(frozen=True, slots=True)
class BoardRebuildIngestionAdapter:
    """Sync rebuild step adapter that enqueues sources for the existing
    consolidation worker to drain. Produces a deterministic structural
    hash + source hash via KG-02.5 primitives so the rebuild report and
    KG-02.4 promotion path receive a real receipt — not a stub one."""

    db_path: Path
    drain_timeout_seconds: float = 900.0
    drain_poll_interval_seconds: float = 0.5
    drain_final_grace_seconds: float = 180.0
    drain_low_depth_threshold: int = 10

    def prepare_board_graph_storage(
        self,
        *,
        board_id: str,
        reason: str,
    ) -> tuple[str, ...]:
        """Quarantine existing board graph files for an explicit rebuild.

        The bootstrap path is fail-closed and must never purge an existing
        graph just because opening it failed. A confirmed rebuild is different:
        the operator already requested replacement, so we move the current
        graph files to quarantine before the deterministic worker bootstraps a
        fresh graph. If quarantine fails, the rebuild step fails and preserves
        the original files.
        """

        from okto_pulse.core.kg.schema import (
            board_kuzu_path,
            purge_board_graph_storage,
        )

        path = board_kuzu_path(board_id)
        targets: list[Path] = []
        if path.exists():
            targets.append(path)
        if path.parent.exists():
            targets.extend(sorted(path.parent.glob(path.name + ".*")))
        if not targets:
            return ()

        moved = tuple(
            purge_board_graph_storage(board_id, reason=reason)
        )
        still_present = [p for p in targets if p.exists()]
        if still_present:
            raise RuntimeError(
                "explicit rebuild could not quarantine existing graph files: "
                + ", ".join(str(p) for p in still_present)
            )
        return moved

    def enqueue_sources(
        self,
        *,
        board_id: str,
        run_id: str,
        sources: Sequence[Mapping[str, Any]],
    ) -> dict[str, int]:
        """UPSERT one ConsolidationQueue row per source. Returns counts
        bucketed by (inserted | reset_to_pending | left_alone). Uses
        ``priority='high'`` because an explicit rebuild is an operator
        recovery action; it must preempt unrelated backlog from other boards
        that may themselves be corrupt."""

        counts = {"inserted": 0, "reset_to_pending": 0, "left_alone": 0}
        if not sources:
            return counts

        with sqlite3.connect(str(self.db_path), timeout=10.0) as conn:
            conn.row_factory = sqlite3.Row
            for row in sources:
                artifact_type = str(row.get("artifact_type", ""))
                artifact_id = str(row.get("id", ""))
                if artifact_type not in _DETERMINISTIC_SOURCE_ARTIFACT_TYPES:
                    continue
                queue_artifact_type = _queue_artifact_type(artifact_type)
                if not artifact_id:
                    continue
                existing = conn.execute(
                    "SELECT id, status FROM consolidation_queue "
                    "WHERE board_id=? AND artifact_type=? AND artifact_id=?",
                    (board_id, queue_artifact_type, artifact_id),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        "INSERT INTO consolidation_queue "
                        "(id, board_id, artifact_type, artifact_id, priority, "
                        "source, status, triggered_at, attempts) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), 0)",
                        (
                            str(uuid.uuid4()),
                            board_id,
                            queue_artifact_type,
                            artifact_id,
                            "high",
                            f"rebuild:{run_id}",
                            "pending",
                        ),
                    )
                    counts["inserted"] += 1
                elif existing["status"] in (
                    "done",
                    "failed",
                    "paused",
                    "pending",
                    "claimed",
                ):
                    conn.execute(
                        "UPDATE consolidation_queue SET "
                        "status='pending', attempts=0, last_error=NULL, "
                        "claimed_by_session_id=NULL, claimed_at=NULL, "
                        "worker_id=NULL, claim_timeout_at=NULL, "
                        "next_retry_at=NULL, priority=?, source=? "
                        "WHERE id=?",
                        ("high", f"rebuild:{run_id}", existing["id"]),
                    )
                    counts["reset_to_pending"] += 1
                else:
                    counts["left_alone"] += 1
            conn.commit()
        return counts

    def build_step_adapter(self, source_resolver):
        """Return a callable conforming to
        ``KGRebuildService.rebuild_step_adapter``. Wraps
        ``DeterministicStructuralRebuilder.as_rebuild_step_adapter`` with
        an extra layer that enqueues the sources for async drain.

        ``source_resolver(req) -> sequence[dict]`` is REQUIRED — it loads
        the sources from the manifest the rebuild service just validated
        (KG-02.2 lifecycle). The deterministic rebuilder consumes the
        same resolved set so the structural_hash is computed over the
        exact rows that get enqueued (no drift).
        """

        from okto_pulse.core.kg.rebuild_deterministic import (
            DeterministicStructuralRebuilder,
        )
        from okto_pulse.core.kg.rebuild_service import RebuildStepResult

        det = DeterministicStructuralRebuilder()
        base_adapter = det.as_rebuild_step_adapter(
            source_resolver=source_resolver,
        )

        def _adapter(req):
            # 1. Run the deterministic hash + counts via KG-02.5. The
            # resolver is called inside base_adapter and ensures
            # sources_payload is wired (fail-closed per val_ebffe9ce if
            # missing).
            base_result = base_adapter(req)
            if not base_result.ok:
                return base_result

            # 2. Resolve sources again (cheap) for the enqueue step. We
            # could thread the resolver result through base_adapter but
            # KG-02.5's RebuildStepResult doesn't carry raw sources; a
            # second call keeps the layering clean.
            sources = tuple(source_resolver(req))
            try:
                affected_files = self.prepare_board_graph_storage(
                    board_id=req.board_id,
                    reason=f"explicit_rebuild:{req.manifest_ref or req.operation}",
                )
            except Exception as exc:
                return RebuildStepResult(
                    ok=False,
                    detail=f"graph_prepare_failed:{type(exc).__name__}",
                    current_kg_generation_id=base_result.current_kg_generation_id,
                    previous_kg_generation_id=base_result.previous_kg_generation_id,
                    structural_hash=base_result.structural_hash,
                    source_hash=base_result.source_hash,
                    counts=base_result.counts,
                    reconciliation_decisions=base_result.reconciliation_decisions,
                    drilldown={
                        **base_result.drilldown,
                        "graph_prepare_error": str(exc),
                    },
                )
            counts_q = self.enqueue_sources(
                board_id=req.board_id,
                run_id=req.manifest_ref or "rebuild",
                sources=sources,
            )

            # 3. Wake the worker so the queue starts draining immediately.
            try:
                from okto_pulse.core.kg.workers.consolidation import (
                    signal_consolidation_worker,
                )
                signal_consolidation_worker()
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning(
                    "kg.rebuild.signal_worker_failed err=%s", exc,
                )

            merged_counts = {
                **base_result.counts,
                "enqueue_inserted": counts_q["inserted"],
                "enqueue_reset_to_pending": counts_q["reset_to_pending"],
                "enqueue_left_alone": counts_q["left_alone"],
            }

            # 4. A rebuild is not complete when rows are merely queued. The
            # generation must not be promoted until the deterministic worker
            # has drained the board backlog and the safe-write lifecycle can
            # close/reopen-probe the materialized graph. Earlier code reported
            # COMPLETED while the actual writes still lived behind an async
            # worker handle, which let a corrupt/unflushed WAL surface later in
            # the UI instead of failing the rebuild run.
            drain = self.drain_until_idle(
                board_id=req.board_id,
                timeout_seconds=self.drain_timeout_seconds,
                poll_interval_seconds=self.drain_poll_interval_seconds,
                final_grace_seconds=self.drain_final_grace_seconds,
                low_depth_threshold=self.drain_low_depth_threshold,
            )
            if not drain["idle"]:
                return RebuildStepResult(
                    ok=False,
                    detail=(
                        "queue_drain_timeout:"
                        f"final_depth={drain['final_depth']}"
                        f" waited_seconds={drain['waited_seconds']}"
                    ),
                    current_kg_generation_id=base_result.current_kg_generation_id,
                    previous_kg_generation_id=base_result.previous_kg_generation_id,
                    affected_files=(
                        tuple(base_result.affected_files) + affected_files
                    ),
                    structural_hash=base_result.structural_hash,
                    source_hash=base_result.source_hash,
                    counts=merged_counts,
                    reconciliation_decisions=base_result.reconciliation_decisions,
                    drilldown={
                        **base_result.drilldown,
                        "graph_prepare": {
                            "quarantined_files": len(affected_files),
                        },
                        "enqueue": counts_q,
                        "queue_drain": drain,
                        "ingestion_mode": "sync_wait_for_worker_drain",
                    },
                )

            return RebuildStepResult(
                ok=True,
                current_kg_generation_id=base_result.current_kg_generation_id,
                previous_kg_generation_id=base_result.previous_kg_generation_id,
                affected_files=(
                    tuple(base_result.affected_files) + affected_files
                ),
                structural_hash=base_result.structural_hash,
                source_hash=base_result.source_hash,
                counts=merged_counts,
                reconciliation_decisions=base_result.reconciliation_decisions,
                drilldown={
                    **base_result.drilldown,
                    "graph_prepare": {
                        "quarantined_files": len(affected_files),
                    },
                    "enqueue": counts_q,
                    "queue_drain": drain,
                    "ingestion_mode": "sync_wait_for_worker_drain",
                },
            )

        return _adapter

    def drain_until_idle(
        self,
        *,
        board_id: str,
        timeout_seconds: float = 60.0,
        poll_interval_seconds: float = 0.5,
        final_grace_seconds: float | None = None,
        low_depth_threshold: int | None = None,
    ) -> dict[str, Any]:
        """Block until the board's ConsolidationQueue has no
        pending/claimed rows or the timeout fires. Returns a snapshot
        with the wait duration and final depth. Optional helper —
        production callers may prefer to return immediately after the
        rebuild promotes the generation and let the worker drain in the
        background.

        A small final grace window avoids a false failed rebuild when
        the queue is nearly drained at the hard timeout. The Pulse SaaS
        E2E rebuild exposed that failure mode: the run timed out with
        four rows still visible, then the worker finished seconds later,
        leaving a healthy graph but no promoted generation. The grace is
        bounded and only applies while the remaining depth is below the
        configured threshold, so a genuinely stuck large backlog still
        fails closed."""

        start = time.monotonic()
        base_timeout = max(0.5, float(timeout_seconds))
        deadline = start + base_timeout
        grace_seconds = max(
            0.0,
            float(
                self.drain_final_grace_seconds
                if final_grace_seconds is None
                else final_grace_seconds
            ),
        )
        low_depth = max(
            0,
            int(
                self.drain_low_depth_threshold
                if low_depth_threshold is None
                else low_depth_threshold
            ),
        )
        grace_applied = False
        grace_reason: str | None = None
        final_depth = -1
        with sqlite3.connect(str(self.db_path), timeout=5.0) as conn:
            while True:
                row = conn.execute(
                    "SELECT COUNT(*) FROM consolidation_queue "
                    "WHERE board_id=? AND status IN ('pending', 'claimed')",
                    (board_id,),
                ).fetchone()
                final_depth = int(row[0]) if row else 0
                if final_depth == 0:
                    break
                now = time.monotonic()
                if now >= deadline:
                    if (
                        not grace_applied
                        and grace_seconds > 0
                        and 0 < final_depth <= low_depth
                    ):
                        grace_applied = True
                        grace_reason = "low_depth_near_timeout"
                        deadline = now + grace_seconds
                        logger.warning(
                            "kg.rebuild.queue_drain_grace board=%s "
                            "final_depth=%s base_timeout_seconds=%s "
                            "grace_seconds=%s",
                            board_id,
                            final_depth,
                            base_timeout,
                            grace_seconds,
                        )
                        continue
                    break
                time.sleep(min(float(poll_interval_seconds), max(0.0, deadline - now)))
        return {
            "final_depth": final_depth,
            "waited_seconds": round(time.monotonic() - start, 2),
            "idle": final_depth == 0,
            "base_timeout_seconds": base_timeout,
            "final_grace_seconds": grace_seconds,
            "low_depth_threshold": low_depth,
            "grace_applied": grace_applied,
            "grace_reason": grace_reason,
        }


__all__ = ["BoardRebuildIngestionAdapter"]
