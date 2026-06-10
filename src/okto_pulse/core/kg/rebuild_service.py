"""KG rebuild service admin lane (KG-02.3, IR ir_03c2a132 + ir_73c3e169).

The rebuild service is the single mutation entry point for the
recovery/rebuild flow. It consumes the KG-01 primitives end-to-end:

1. Validate confirmation token (KG-02.2 ``RebuildConfirmationStore``).
2. Load + revalidate manifest (KG-02.2 ``KGRebuildSourceManifest``)
   so drift between preflight and run aborts the mutation.
3. Acquire the per-board single-writer lock under the ADMIN LANE
   (KG-01.3 ``KGSingleWriterLock`` with ``admin_lane=True``).
4. Enter ``under_safe_write`` so any downstream mutation primitive
   (commit_consolidation, etc.) sees the active guard
   (KG-01.3.1 write_barrier).
5. Run the safe write lifecycle (KG-01.3 ``KGSafeWriteLifecycle``)
   so checkpoint→flush→fsync→close_reopen_probe applies around the
   rebuild step.
6. On reset: quarantine BEFORE purge (KG-01.4
   ``KGQuarantineService``).
7. Always release the lock and emit the audit trail (TR12) +
   counter (OR or_37cebd03).

This module does NOT implement the deterministic structural rebuild
itself — that's KG-02.5 (``DeterministicStructuralRebuilder``). KG-02.3
ships the orchestration boundary; the step is injected as
``rebuild_step_adapter``.

Outcomes (RebuildOutcome enum) per AC15 + OR or_37cebd03:

* ``STARTED`` — lock acquired, step about to run.
* ``COMPLETED`` — rebuild step + lifecycle returned APPLIED.
* ``FAILED`` — rebuild step or lifecycle returned FAILED.
* ``REBUILD_FAILED`` — orchestrator-level failure (exception escaping
  the step adapter, etc) where the storage is in a known-incomplete
  state and the operator must investigate.
* ``CONFIRMATION_REQUIRED`` — confirmation invalid (missing/expired/
  scope_mismatch/replayed). No mutation attempted.
* ``MANIFEST_DRIFT`` — source set changed between preflight and run.
  No mutation attempted.
* ``LOCK_CONTENTION`` — admin lane could not acquire the lock
  exclusively (another writer in flight). Retryable.

Audit trail (TR12): every run persists an entry to
``<base>/rebuild/audit/<run_id>.json`` with
``confirmation_id, actor_id, reason, affected_files,
previous_kg_generation_id, current_kg_generation_id, outcome``.
``current_kg_generation_id`` is set by KG-02.4 — KG-02.3 leaves it
None on outcomes other than COMPLETED.
"""

from __future__ import annotations

import json
import logging
import secrets
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger("okto_pulse.kg.rebuild_service")


REBUILD_DIRNAME = "rebuild"
AUDIT_DIRNAME = "audit"


class RebuildOutcome(str, Enum):
    """Outcomes per AC15 + OR or_37cebd03 label vocabulary."""

    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    REBUILD_FAILED = "rebuild_failed"
    CONFIRMATION_REQUIRED = "confirmation_required"
    MANIFEST_DRIFT = "manifest_drift"
    LOCK_CONTENTION = "lock_contention"
    # val_dfdff0b8: fail-closed for any operation KG-02.3 does NOT
    # fully implement. reset/quarantine/promote/rollback/reindex
    # land here until KG-02.4+ ships the real paths.
    UNSUPPORTED_OPERATION = "unsupported_operation"
    # KG-02.4: report persistence is the terminal-state gate. If it
    # fails the safe previous generation is preserved (br_82deef11).
    REPORT_PERSIST_FAILED = "report_persist_failed"
    FAILED_ORPHAN_VALIDATION = "failed_orphan_validation"


class RebuildBlockReason(str, Enum):
    """Bounded reason vocabulary for OR or_37cebd03 ``reason`` label."""

    CONFIRMATION_INVALID = "confirmation_invalid"
    MANIFEST_NOT_FOUND = "manifest_not_found"
    MANIFEST_DRIFT = "manifest_drift"
    LOCK_CONTENTION = "lock_contention"
    LIFECYCLE_FAILED = "lifecycle_failed"
    STEP_EXCEPTION = "step_exception"
    QUARANTINE_FAILED = "quarantine_failed"
    OPERATION_PENDING_KG02_4 = "operation_pending_kg02_4"
    # KG-02.4 — report-first terminal gate reasons.
    REPORT_PERSIST_STORE_FAILED = "report_persist_store_failed"
    REPORT_PERSIST_SENSITIVE_REJECTED = "report_persist_sensitive_rejected"
    GENERATION_PROMOTION_BLOCKED = "generation_promotion_blocked"
    ORPHAN_VALIDATION_FAILED = "orphan_validation_failed"
    OK = "ok"


# val_dfdff0b8 rework: KG-02.3 fully implements ONLY the ``rebuild``
# operation. reset/quarantine/promote/rollback/reindex_discovery land
# in KG-02.4 (generation repo + report-first) and KG-02.6 (discovery
# reindex). Until then ANY non-rebuild operation must short-circuit
# with UNSUPPORTED_OPERATION BEFORE the lock is taken so we never
# emit a silent "completed" for a destructive op.
SUPPORTED_REBUILD_OPERATIONS: frozenset[str] = frozenset({"rebuild"})


@dataclass(frozen=True, slots=True)
class RebuildRunResult:
    """Frozen response for ``KGRebuildService.run``."""

    run_id: str
    outcome: str  # RebuildOutcome value
    reason: str  # RebuildBlockReason value
    audit_ref: str
    previous_kg_generation_id: str | None
    current_kg_generation_id: str | None
    started_at: str
    finished_at: str
    affected_files: tuple[str, ...] = field(default_factory=tuple)
    # KG-02.4 — report_ref is the durable receipt produced by
    # RebuildReportStore.persist; populated for terminal outcomes when
    # the report primitives are wired.
    report_ref: str | None = None
    report_id: str | None = None
    publishable_status: str | None = None
    promotion_outcome: str | None = None
    operator_action: str | None = None
    event_emitted: bool = False


# --- Counter (OR or_37cebd03) ------------------------------------------------

_REBUILD_LABELS = ("board_id", "status", "reason")
_rebuild_counter: dict[tuple[str, str, str], int] = {}
_rebuild_counter_lock = threading.Lock()


def _bump_rebuild(*, board_id: str, status: str, reason: str) -> None:
    key = (board_id, status, reason)
    with _rebuild_counter_lock:
        _rebuild_counter[key] = _rebuild_counter.get(key, 0) + 1


def get_rebuild_run_count(
    board_id: str, status: str, *, reason: str | None = None
) -> int:
    with _rebuild_counter_lock:
        total = 0
        for (b, st, rsn), value in _rebuild_counter.items():
            if b != board_id or st != status:
                continue
            if reason is not None and rsn != reason:
                continue
            total += value
        return total


def get_rebuild_run_samples() -> list[dict[str, Any]]:
    with _rebuild_counter_lock:
        return [
            {"board_id": b, "status": st, "reason": rsn, "count": value}
            for (b, st, rsn), value in _rebuild_counter.items()
        ]


def get_rebuild_run_counter_labels() -> tuple[str, ...]:
    return _REBUILD_LABELS


def reset_rebuild_run_counter() -> None:
    with _rebuild_counter_lock:
        _rebuild_counter.clear()


# --- Adapter types -----------------------------------------------------------

# Rebuild step adapter: receives the manifest + actor context and
# performs the actual structural rebuild. KG-02.5 wires the real
# deterministic rebuilder; KG-02.3 ships a stub that returns ok=True.
# Returns RebuildStepResult.
@dataclass(frozen=True, slots=True)
class RebuildStepInput:
    board_id: str
    manifest_ref: str
    source_set_hash: str
    actor_id: str
    operation: str
    owner_token: str  # KG-01 single-writer lock token
    # KG-02.4 — the orchestrator pre-supplies the candidate generation
    # so the step adapter can stamp the new id into its outputs without
    # generating its own (UUID v4 / TR3 lives in the repository).
    previous_kg_generation_id: str | None = None
    candidate_kg_generation_id: str | None = None


@dataclass(frozen=True, slots=True)
class RebuildStepResult:
    ok: bool
    detail: str | None = None
    affected_files: tuple[str, ...] = field(default_factory=tuple)
    previous_kg_generation_id: str | None = None
    # KG-02.4 — populated by the orchestrator when a candidate was
    # supplied. The step adapter may echo it forward; KG-02.5 ships
    # the real structural_hash + source_hash from the deterministic
    # rebuilder.
    current_kg_generation_id: str | None = None
    structural_hash: str | None = None
    source_hash: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    reconciliation_decisions: tuple[dict[str, Any], ...] = field(
        default_factory=tuple
    )
    drilldown: dict[str, Any] = field(default_factory=dict)


RebuildStepAdapter = Callable[[RebuildStepInput], RebuildStepResult]


def _default_step_adapter(req: RebuildStepInput) -> RebuildStepResult:
    """Stub step — accepts and returns ok. KG-02.5 replaces with the
    deterministic rebuilder. Tests inject their own to exercise
    failure paths."""
    return RebuildStepResult(
        ok=True,
        current_kg_generation_id=req.candidate_kg_generation_id,
    )


# KG-02.4 — kg.rebuilt event emitter (TR8). Default is no-op; production
# wires the audit pipeline (KG-02.7).
KGRebuiltEventEmitter = Callable[[dict[str, Any]], None]
OrphanScanProvider = Callable[[str, str | None], Any]


def _default_event_emitter(_event: dict[str, Any]) -> None:
    return None


# --- The service -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class KGRebuildService:
    """Orchestrator that wraps a rebuild step in the KG-01 admin lane.

    Adapters are injected: tests pass fakes for the lock, quarantine,
    confirmation store, manifest store and step. Production wires the
    real KG-01.1-5 primitives.
    """

    base_dir: Path
    single_writer_lock: Any  # KGSingleWriterLock
    safe_write_lifecycle: Any  # KGSafeWriteLifecycle
    quarantine_service: Any  # KGQuarantineService | None
    confirmation_store: Any  # RebuildConfirmationStore
    manifest_store: Any  # KGRebuildSourceManifest
    rebuild_step_adapter: RebuildStepAdapter = _default_step_adapter
    # Source enumerator used to revalidate manifest at run time.
    source_enumerator: Any | None = None
    # KG-02.4 — generation + report primitives. Optional so KG-02.3
    # callers that haven't migrated yet keep working; when wired the
    # orchestrator runs the report-first terminal gate before audit.
    generation_repository: Any | None = None  # KGGenerationRepository
    promotion_guard: Any | None = None  # KGGenerationPromotionGuard
    report_store: Any | None = None  # RebuildReportStore
    terminal_state_guard: Any | None = None  # RebuildReportTerminalStateGuard
    event_emitter: KGRebuiltEventEmitter = _default_event_emitter
    orphan_scan_provider: OrphanScanProvider | None = None
    # Lock TTL while the rebuild runs. Default 1h — enough for the
    # 1000-iter stress at KG-01.6 plus headroom.
    lock_ttl_seconds: int = 3600

    # --- public API --------------------------------------------------------

    def run(
        self,
        *,
        confirmation_id: str,
        board_id: str,
        actor_id: str,
        operation: str,
        preflight_hash: str,
        manifest_ref: str,
        reason: str,
    ) -> RebuildRunResult:
        """Execute the rebuild under the KG-01 admin lane.

        TR13/TR12 invariants:
        * No mutation if confirmation invalid.
        * Lock acquired with ``admin_lane=True``.
        * ``under_safe_write`` guard active during the step.
        * Audit trail written before counter bump.
        """
        from okto_pulse.core.kg.rebuild_confirmation import (
            ConfirmationOutcome,
        )
        from okto_pulse.core.kg.write_barrier import under_safe_write

        run_id = f"run_{secrets.token_urlsafe(12)}"
        started_at = datetime.now(timezone.utc)

        # 0. val_dfdff0b8 fail-closed: KG-02.3 only implements rebuild.
        # Reject any other operation BEFORE consuming the token, so a
        # destructive op never appears completed=true via stub adapter.
        if operation not in SUPPORTED_REBUILD_OPERATIONS:
            return self._emit_audit_and_counter(
                run_id=run_id,
                outcome=RebuildOutcome.UNSUPPORTED_OPERATION,
                reason=RebuildBlockReason.OPERATION_PENDING_KG02_4,
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                confirmation_id=confirmation_id,
                manifest_ref=manifest_ref,
                user_reason=reason,
                started_at=started_at,
                affected_files=(),
                previous_kg_generation_id=None,
                current_kg_generation_id=None,
                detail=(
                    f"operation={operation!r} not implemented by KG-02.3; "
                    f"supported={sorted(SUPPORTED_REBUILD_OPERATIONS)}"
                ),
            )

        # 1. Validate confirmation. consume() unlinks atomic — second
        # attempt with the same id returns MISSING/REPLAYED.
        conf_result = self.confirmation_store.consume(
            confirmation_id=confirmation_id,
            expected_board_id=board_id,
            expected_actor_id=actor_id,
            expected_operation=operation,
            expected_preflight_hash=preflight_hash,
            expected_manifest_ref=manifest_ref,
        )
        if conf_result.outcome != ConfirmationOutcome.CONSUMED.value:
            return self._emit_audit_and_counter(
                run_id=run_id,
                outcome=RebuildOutcome.CONFIRMATION_REQUIRED,
                reason=RebuildBlockReason.CONFIRMATION_INVALID,
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                confirmation_id=confirmation_id,
                manifest_ref=manifest_ref,
                user_reason=reason,
                started_at=started_at,
                affected_files=(),
                previous_kg_generation_id=None,
                current_kg_generation_id=None,
                detail=f"confirmation_outcome={conf_result.outcome}",
            )

        # 2. Load manifest. We already validated the ref in /confirm
        # but the file may have been tampered with between confirm and
        # run; defence in depth.
        manifest = self.manifest_store.load(manifest_ref)
        if manifest is None:
            return self._emit_audit_and_counter(
                run_id=run_id,
                outcome=RebuildOutcome.REBUILD_FAILED,
                reason=RebuildBlockReason.MANIFEST_NOT_FOUND,
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                confirmation_id=confirmation_id,
                manifest_ref=manifest_ref,
                user_reason=reason,
                started_at=started_at,
                affected_files=(),
                previous_kg_generation_id=None,
                current_kg_generation_id=None,
                detail="manifest gone after confirmation",
            )

        # 3. Revalidate manifest against fresh enumeration if a source
        # enumerator is wired. Drift → abort.
        if self.source_enumerator is not None:
            current_source_set = self.source_enumerator.enumerate(
                board_id=board_id
            )
            if not self.manifest_store.revalidate(
                manifest=manifest, current_source_set=current_source_set
            ):
                return self._emit_audit_and_counter(
                    run_id=run_id,
                    outcome=RebuildOutcome.MANIFEST_DRIFT,
                    reason=RebuildBlockReason.MANIFEST_DRIFT,
                    board_id=board_id,
                    actor_id=actor_id,
                    operation=operation,
                    confirmation_id=confirmation_id,
                    manifest_ref=manifest_ref,
                    user_reason=reason,
                    started_at=started_at,
                    affected_files=(),
                    previous_kg_generation_id=None,
                    current_kg_generation_id=None,
                    detail="source_set_hash drift between preflight and run",
                )

        # 4. Acquire lock under admin lane (KG-01.3 TR7). Other writers
        # MUST drain — there is no bypass; if the contention persists
        # the operator retries.
        acquisition = self.single_writer_lock.acquire(
            board_id=board_id,
            operation=f"kg02_rebuild:{operation}",
            owner_id=actor_id,
            ttl_seconds=self.lock_ttl_seconds,
            admin_lane=True,
        )
        if not acquisition.acquired:
            return self._emit_audit_and_counter(
                run_id=run_id,
                outcome=RebuildOutcome.LOCK_CONTENTION,
                reason=RebuildBlockReason.LOCK_CONTENTION,
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                confirmation_id=confirmation_id,
                manifest_ref=manifest_ref,
                user_reason=reason,
                started_at=started_at,
                affected_files=(),
                previous_kg_generation_id=None,
                current_kg_generation_id=None,
                detail=f"current_owner={acquisition.current_owner}",
            )

        # 5. Run inside admin guard + safe lifecycle.
        owner_token = acquisition.owner_token
        affected: tuple[str, ...] = ()
        previous_generation: str | None = None
        current_generation: str | None = None
        step_result_holder: dict[str, Any] = {}

        # KG-02.4 — pre-supply candidate generation if the repository is
        # wired, so the step adapter and downstream report carry a real
        # UUID v4 (TR3). Previous generation pointer comes from the
        # repository so promotion can detect drift.
        candidate_generation_id: str | None = None
        if self.generation_repository is not None:
            from okto_pulse.core.kg.rebuild_generation import (
                generate_kg_generation_id,
            )

            try:
                previous_generation = self.generation_repository.get_current(
                    board_id
                )
            except Exception as exc:
                logger.error(
                    "kg.rebuild.previous_generation_read_failed board=%s err=%s",
                    board_id, exc,
                )
            candidate_generation_id = generate_kg_generation_id()

        try:
            with under_safe_write(board_id, owner_token, operation):
                # Optional pre-step quarantine (reset/rebuild may need
                # to move existing graph files first). The quarantine
                # service is read off self and only invoked when the
                # caller supplied a non-None value.
                # KG-02.3 supplies the boundary — actual storage
                # mutation happens inside the step adapter.

                # 5a. Run the rebuild step.
                step_input = RebuildStepInput(
                    board_id=board_id,
                    manifest_ref=manifest_ref,
                    source_set_hash=manifest.source_set_hash,
                    actor_id=actor_id,
                    operation=operation,
                    owner_token=owner_token,
                    previous_kg_generation_id=previous_generation,
                    candidate_kg_generation_id=candidate_generation_id,
                )
                try:
                    step_result = self.rebuild_step_adapter(step_input)
                except Exception as exc:
                    return self._finalise_with_release(
                        run_id=run_id,
                        outcome=RebuildOutcome.REBUILD_FAILED,
                        reason=RebuildBlockReason.STEP_EXCEPTION,
                        board_id=board_id,
                        actor_id=actor_id,
                        operation=operation,
                        confirmation_id=confirmation_id,
                        manifest_ref=manifest_ref,
                        user_reason=reason,
                        started_at=started_at,
                        owner_token=owner_token,
                        affected_files=(),
                        previous_kg_generation_id=previous_generation,
                        current_kg_generation_id=None,
                        detail=f"step_exception={type(exc).__name__}",
                        triggered_by=actor_id,
                        step_result=None,
                        candidate_kg_generation_id=candidate_generation_id,
                    )

                affected = step_result.affected_files
                step_result_holder["result"] = step_result
                if step_result.previous_kg_generation_id is not None:
                    previous_generation = step_result.previous_kg_generation_id
                current_generation = (
                    step_result.current_kg_generation_id
                    or candidate_generation_id
                )

                if not step_result.ok:
                    return self._finalise_with_release(
                        run_id=run_id,
                        outcome=RebuildOutcome.FAILED,
                        reason=RebuildBlockReason.LIFECYCLE_FAILED,
                        board_id=board_id,
                        actor_id=actor_id,
                        operation=operation,
                        confirmation_id=confirmation_id,
                        manifest_ref=manifest_ref,
                        user_reason=reason,
                        started_at=started_at,
                        owner_token=owner_token,
                        affected_files=affected,
                        previous_kg_generation_id=previous_generation,
                        current_kg_generation_id=None,
                        detail=step_result.detail or "step returned ok=False",
                        triggered_by=actor_id,
                        step_result=step_result,
                        candidate_kg_generation_id=candidate_generation_id,
                    )

                # 5b. Run safe write lifecycle (checkpoint/flush/fsync/
                # close_reopen_probe). Required by IR ir_73c3e169.
                lifecycle_result = self.safe_write_lifecycle.apply(
                    board_id=board_id,
                    graph_type="board_graph",
                    operation=f"kg02_rebuild:{operation}",
                    owner_token=owner_token,
                    mutation_ref=run_id,
                )
                if lifecycle_result.status.value != "applied":
                    return self._finalise_with_release(
                        run_id=run_id,
                        outcome=RebuildOutcome.FAILED,
                        reason=RebuildBlockReason.LIFECYCLE_FAILED,
                        board_id=board_id,
                        actor_id=actor_id,
                        operation=operation,
                        confirmation_id=confirmation_id,
                        manifest_ref=manifest_ref,
                        user_reason=reason,
                        started_at=started_at,
                        owner_token=owner_token,
                        affected_files=affected,
                        previous_kg_generation_id=previous_generation,
                        current_kg_generation_id=None,
                        detail=(
                            f"lifecycle_status={lifecycle_result.status.value}"
                            f" failed_step={lifecycle_result.failed_step}"
                        ),
                        triggered_by=actor_id,
                        step_result=step_result,
                        candidate_kg_generation_id=candidate_generation_id,
                    )
        except Exception as exc:
            # Catch-all so we never leak the lock.
            return self._finalise_with_release(
                run_id=run_id,
                outcome=RebuildOutcome.REBUILD_FAILED,
                reason=RebuildBlockReason.STEP_EXCEPTION,
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                confirmation_id=confirmation_id,
                manifest_ref=manifest_ref,
                user_reason=reason,
                started_at=started_at,
                owner_token=owner_token,
                affected_files=affected,
                previous_kg_generation_id=previous_generation,
                current_kg_generation_id=None,
                detail=f"orchestrator_exception={type(exc).__name__}",
                triggered_by=actor_id,
                step_result=step_result_holder.get("result"),
                candidate_kg_generation_id=candidate_generation_id,
            )

        # 6. Happy path: COMPLETED.
        return self._finalise_with_release(
            run_id=run_id,
            outcome=RebuildOutcome.COMPLETED,
            reason=RebuildBlockReason.OK,
            board_id=board_id,
            actor_id=actor_id,
            operation=operation,
            confirmation_id=confirmation_id,
            manifest_ref=manifest_ref,
            user_reason=reason,
            started_at=started_at,
            owner_token=owner_token,
            affected_files=affected,
            previous_kg_generation_id=previous_generation,
            current_kg_generation_id=current_generation,
            detail=None,
            triggered_by=actor_id,
            step_result=step_result_holder.get("result"),
            candidate_kg_generation_id=candidate_generation_id,
        )

    # --- internals ---------------------------------------------------------

    def _finalise_with_release(
        self,
        *,
        run_id: str,
        outcome: RebuildOutcome,
        reason: RebuildBlockReason,
        board_id: str,
        actor_id: str,
        operation: str,
        confirmation_id: str,
        manifest_ref: str,
        user_reason: str,
        started_at: datetime,
        owner_token: str,
        affected_files: tuple[str, ...],
        previous_kg_generation_id: str | None,
        current_kg_generation_id: str | None,
        detail: str | None,
        triggered_by: str | None = None,
        step_result: RebuildStepResult | None = None,
        candidate_kg_generation_id: str | None = None,
    ) -> RebuildRunResult:
        # Always release the lock — even on failure. The lock is the
        # KG-01 single-writer; leaving it orphaned blocks every future
        # writer until TTL.
        try:
            self.single_writer_lock.release(
                board_id=board_id, owner_token=owner_token
            )
        except Exception as exc:
            logger.error(
                "kg.rebuild.lock_release_failed board=%s err=%s",
                board_id, exc,
            )

        # KG-02.4 report-first terminal gate. Only runs when the four
        # primitives are wired; otherwise we fall back to the KG-02.3
        # finalisation (audit + counter, no promotion).
        report_first_decision = self._apply_report_first_gate(
            run_id=run_id,
            outcome=outcome,
            reason=reason,
            board_id=board_id,
            actor_id=actor_id,
            operation=operation,
            manifest_ref=manifest_ref,
            user_reason=user_reason,
            started_at=started_at,
            affected_files=affected_files,
            previous_kg_generation_id=previous_kg_generation_id,
            candidate_kg_generation_id=(
                current_kg_generation_id or candidate_kg_generation_id
            ),
            detail=detail,
            triggered_by=triggered_by or actor_id,
            step_result=step_result,
        )

        return self._emit_audit_and_counter(
            run_id=run_id,
            outcome=report_first_decision["outcome"],
            reason=report_first_decision["reason"],
            board_id=board_id,
            actor_id=actor_id,
            operation=operation,
            confirmation_id=confirmation_id,
            manifest_ref=manifest_ref,
            user_reason=user_reason,
            started_at=started_at,
            affected_files=affected_files,
            previous_kg_generation_id=previous_kg_generation_id,
            current_kg_generation_id=report_first_decision[
                "current_kg_generation_id"
            ],
            detail=report_first_decision["detail"] or detail,
            report_ref=report_first_decision["report_ref"],
            report_id=report_first_decision["report_id"],
            publishable_status=report_first_decision["publishable_status"],
            promotion_outcome=report_first_decision["promotion_outcome"],
            operator_action=report_first_decision["operator_action"],
            event_emitted=report_first_decision["event_emitted"],
        )

    # --- KG-02.4 report-first terminal gate -------------------------------

    def _apply_report_first_gate(
        self,
        *,
        run_id: str,
        outcome: RebuildOutcome,
        reason: RebuildBlockReason,
        board_id: str,
        actor_id: str,
        operation: str,
        manifest_ref: str,
        user_reason: str,
        started_at: datetime,
        affected_files: tuple[str, ...],
        previous_kg_generation_id: str | None,
        candidate_kg_generation_id: str | None,
        detail: str | None,
        triggered_by: str,
        step_result: RebuildStepResult | None,
    ) -> dict[str, Any]:
        """If KG-02.4 primitives are wired, build + persist a report,
        run the terminal-state guard, optionally promote the generation,
        and emit ``kg.rebuilt``. Returns a dict that updates the final
        ``RebuildRunResult``.

        When ANY of (report_store, terminal_state_guard) is missing the
        gate degrades to the KG-02.3 behaviour and returns the inputs
        unchanged. This keeps the orchestrator backwards-compatible for
        callers that have not yet wired the new primitives.
        """

        default = {
            "outcome": outcome,
            "reason": reason,
            "current_kg_generation_id": candidate_kg_generation_id
            if outcome is RebuildOutcome.COMPLETED
            else None,
            "report_ref": None,
            "report_id": None,
            "publishable_status": None,
            "promotion_outcome": None,
            "operator_action": None,
            "event_emitted": False,
            "detail": detail,
        }

        if self.report_store is None or self.terminal_state_guard is None:
            return default

        # Map RebuildOutcome -> candidate_terminal_status. Outcomes that
        # never publish a terminal health state (CONFIRMATION_REQUIRED,
        # MANIFEST_DRIFT, LOCK_CONTENTION, UNSUPPORTED_OPERATION) bypass
        # the gate — no report needed because the operation never
        # touched storage.
        candidate_terminal = _candidate_terminal_status_for(outcome)
        if candidate_terminal is None:
            return default

        zero_orphan_validation: dict[str, Any] | None = None
        if outcome is RebuildOutcome.COMPLETED:
            zero_orphan_validation = self._build_zero_orphan_validation(
                board_id=board_id,
                generation_id=candidate_kg_generation_id,
            )
            if zero_orphan_validation.get("integrity_warning") or (
                zero_orphan_validation.get("zero_orphan_validation")
                == "unavailable"
            ):
                candidate_terminal = RebuildOutcome.FAILED_ORPHAN_VALIDATION.value

        # Build the canonical payload. step_result may be None for the
        # paths where the step never ran (e.g. exception inside the
        # admin guard before the adapter call). In that case we surface
        # an empty drilldown but still persist the report so the
        # operator gets an audit-grade artefact.
        finished_for_summary = datetime.now(timezone.utc).isoformat()
        from okto_pulse.core.kg.rebuild_report import (
            RebuildReportPayload,
            RebuildReportSummary,
            ReportPersistOutcome,
        )

        summary_counts = dict(step_result.counts) if step_result else {}
        if zero_orphan_validation is not None:
            summary_counts["orphan_count"] = int(
                zero_orphan_validation.get("orphan_count") or 0
            )
        summary = RebuildReportSummary(
            board_id=board_id,
            run_id=run_id,
            status=candidate_terminal,
            started_at=started_at.isoformat(),
            finished_at=finished_for_summary,
            counts=summary_counts,
            triggered_by=triggered_by,
            previous_kg_generation_id=previous_kg_generation_id,
            kg_generation_id=candidate_kg_generation_id
            if candidate_terminal == "completed"
            else None,
        )
        hashes: dict[str, str] = {}
        if step_result and step_result.structural_hash:
            hashes["structural_hash"] = step_result.structural_hash
        if step_result and step_result.source_hash:
            hashes["source_hash"] = step_result.source_hash
        drilldown = dict(step_result.drilldown) if step_result else {}
        if zero_orphan_validation is not None:
            drilldown["zero_orphan_validation"] = zero_orphan_validation
        payload = RebuildReportPayload(
            summary=summary,
            hashes=hashes,
            source_refs=(manifest_ref,),
            reconciliation_decisions=tuple(
                step_result.reconciliation_decisions if step_result else ()
            ),
            drilldown=drilldown,
            operator_notes=user_reason or None,
        )

        persist_result = self.report_store.persist(payload=payload)
        terminal_decision = self.terminal_state_guard.require_report_ref(
            board_id=board_id,
            run_id=run_id,
            candidate_terminal_status=candidate_terminal,
            report_persist_result=persist_result,
            previous_kg_generation_id=previous_kg_generation_id,
            kg_generation_id=candidate_kg_generation_id,
        )

        final_outcome: RebuildOutcome
        final_reason: RebuildBlockReason
        promoted_generation: str | None = None
        promotion_outcome_value: str | None = None
        operator_action = terminal_decision.operator_action
        event_emitted = False

        if persist_result.outcome != ReportPersistOutcome.STORED.value:
            # br_82deef11: store failure leaves previous safe generation
            # active and surfaces report_persist_failed.
            if persist_result.outcome == ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value:
                final_reason = RebuildBlockReason.REPORT_PERSIST_SENSITIVE_REJECTED
            else:
                final_reason = RebuildBlockReason.REPORT_PERSIST_STORE_FAILED
            final_outcome = RebuildOutcome.REPORT_PERSIST_FAILED
        else:
            if candidate_terminal == RebuildOutcome.FAILED_ORPHAN_VALIDATION.value:
                final_outcome = RebuildOutcome.FAILED_ORPHAN_VALIDATION
                final_reason = RebuildBlockReason.ORPHAN_VALIDATION_FAILED
                operator_action = operator_action or "run_orphan_backfill"
            else:
                final_outcome = outcome
                final_reason = reason

            # Promotion only when the original outcome was COMPLETED.
            # Other terminal statuses (FAILED, REBUILD_FAILED) MUST
            # persist a report for auditability but MUST NOT promote.
            if (
                outcome is RebuildOutcome.COMPLETED
                and self.generation_repository is not None
                and candidate_kg_generation_id is not None
                and terminal_decision.promotion_allowed
            ):
                guard_eligible = True
                if self.promotion_guard is not None:
                    structural_hash = (
                        (step_result.structural_hash if step_result else None)
                        or ""
                    )
                    source_hash = (
                        (step_result.source_hash if step_result else None)
                        or ""
                    )
                    evaluation = self.promotion_guard.evaluate(
                        board_id=board_id,
                        previous_kg_generation_id=previous_kg_generation_id,
                        kg_generation_id=candidate_kg_generation_id,
                        report_ref=persist_result.report_ref,
                        status=candidate_terminal,
                        structural_hash=structural_hash or "structural-hash-pending",
                        source_hash=source_hash or "source-hash-pending",
                    )
                    guard_eligible = evaluation.eligible
                    if not guard_eligible:
                        operator_action = (
                            evaluation.required_operator_action
                            or "promotion_blocked_by_guard"
                        )
                        final_outcome = RebuildOutcome.REPORT_PERSIST_FAILED
                        final_reason = (
                            RebuildBlockReason.GENERATION_PROMOTION_BLOCKED
                        )

                if guard_eligible:
                    promo = self.generation_repository.promote_current(
                        board_id=board_id,
                        previous_kg_generation_id=previous_kg_generation_id,
                        kg_generation_id=candidate_kg_generation_id,
                        report_ref=persist_result.report_ref,
                        status=candidate_terminal,
                        structural_hash=(
                            (step_result.structural_hash if step_result else None)
                            or "structural-hash-pending"
                        ),
                        source_hash=(
                            (step_result.source_hash if step_result else None)
                            or "source-hash-pending"
                        ),
                        promoted_by=triggered_by,
                        run_id=run_id,
                    )
                    promotion_outcome_value = promo.outcome
                    if promo.outcome == "promoted":
                        promoted_generation = promo.current_kg_generation_id
                    else:
                        final_outcome = RebuildOutcome.REPORT_PERSIST_FAILED
                        final_reason = (
                            RebuildBlockReason.GENERATION_PROMOTION_BLOCKED
                        )
                        operator_action = (
                            operator_action or "promotion_repository_rejected"
                        )

            # TR8 — kg.rebuilt event emitted whenever the report
            # persisted, regardless of promotion outcome. Operators need
            # the event to know terminal state was reached.
            event_payload = {
                "event": "kg.rebuilt",
                "board_id": board_id,
                "previous_kg_generation_id": previous_kg_generation_id,
                "kg_generation_id": promoted_generation
                or (
                    candidate_kg_generation_id
                    if outcome is RebuildOutcome.COMPLETED
                    else None
                ),
                "triggered_by": triggered_by,
                "started_at": started_at.isoformat(),
                "finished_at": finished_for_summary,
                "status": final_outcome.value
                if final_outcome is RebuildOutcome.REPORT_PERSIST_FAILED
                else candidate_terminal,
                "counts": dict(step_result.counts) if step_result else {},
                "report_ref": persist_result.report_ref,
                # bug b4c6920c: manifest_ref carried so the
                # event_emitter source_resolver can re-load the
                # source set and drive CognitivePendingMarker.
                "manifest_ref": manifest_ref,
                "run_id": run_id,
            }
            try:
                self.event_emitter(event_payload)
                event_emitted = True
            except Exception as exc:
                logger.error(
                    "kg.rebuild.event_emit_failed run_id=%s err=%s",
                    run_id, exc,
                )

        return {
            "outcome": final_outcome,
            "reason": final_reason,
            "current_kg_generation_id": promoted_generation,
            "report_ref": persist_result.report_ref,
            "report_id": persist_result.report_id,
            "publishable_status": terminal_decision.publishable_status,
            "promotion_outcome": promotion_outcome_value,
            "operator_action": operator_action,
            "event_emitted": event_emitted,
            "detail": persist_result.detail or detail,
        }

    def _build_zero_orphan_validation(
        self, *, board_id: str, generation_id: str | None
    ) -> dict[str, Any]:
        from okto_pulse.core.kg.orphan_integrity import (
            build_orphan_integrity_projection,
        )

        if self.orphan_scan_provider is None:
            return build_orphan_integrity_projection(
                None,
                not_evaluated_reason="orphan_scan_provider_not_configured",
            ).to_safe_dict()
        try:
            report = self.orphan_scan_provider(board_id, generation_id)
            return build_orphan_integrity_projection(report).to_safe_dict()
        except Exception as exc:
            logger.warning(
                "kg.rebuild.orphan_validation_failed board=%s err=%s",
                board_id, exc,
            )
            return build_orphan_integrity_projection(
                None,
                scan_error=type(exc).__name__,
            ).to_safe_dict()

    def _emit_audit_and_counter(
        self,
        *,
        run_id: str,
        outcome: RebuildOutcome,
        reason: RebuildBlockReason,
        board_id: str,
        actor_id: str,
        operation: str,
        confirmation_id: str,
        manifest_ref: str,
        user_reason: str,
        started_at: datetime,
        affected_files: tuple[str, ...],
        previous_kg_generation_id: str | None,
        current_kg_generation_id: str | None,
        detail: str | None,
        report_ref: str | None = None,
        report_id: str | None = None,
        publishable_status: str | None = None,
        promotion_outcome: str | None = None,
        operator_action: str | None = None,
        event_emitted: bool = False,
    ) -> RebuildRunResult:
        finished_at = datetime.now(timezone.utc)
        audit_dir = self.base_dir / REBUILD_DIRNAME / AUDIT_DIRNAME
        audit_dir.mkdir(parents=True, exist_ok=True)
        audit_path = audit_dir / f"{run_id}.json"
        # val_8fa8019d rework: NEVER persist the raw confirmation_id in
        # the legacy run audit. Replace it with the canonical SHA256
        # fingerprint produced by KG-02.7 so operators can still join
        # audit rows by token without exposing the secret. Best-effort:
        # if the fingerprint helper fails (impossible in practice for
        # a non-empty string), fall back to the literal "redacted".
        try:
            from okto_pulse.core.kg.rebuild_audit import (
                confirmation_fingerprint,
            )

            confirmation_ref = confirmation_fingerprint(confirmation_id)
        except Exception:
            confirmation_ref = "redacted"
        audit_payload = {
            "run_id": run_id,
            "outcome": outcome.value,
            "reason": reason.value,
            "board_id": board_id,
            "actor_id": actor_id,
            "operation": operation,
            "confirmation_ref": confirmation_ref,
            "manifest_ref": manifest_ref,
            "user_reason": user_reason,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "affected_files": list(affected_files),
            "previous_kg_generation_id": previous_kg_generation_id,
            "current_kg_generation_id": current_kg_generation_id,
            "report_ref": report_ref,
            "report_id": report_id,
            "publishable_status": publishable_status,
            "promotion_outcome": promotion_outcome,
            "operator_action": operator_action,
            "event_emitted": event_emitted,
            "detail": detail,
        }
        try:
            with audit_path.open("w", encoding="utf-8") as fh:
                json.dump(audit_payload, fh, indent=2)
        except Exception as exc:
            logger.error(
                "kg.rebuild.audit_persist_failed run_id=%s err=%s",
                run_id, exc,
            )

        _bump_rebuild(board_id=board_id, status=outcome.value, reason=reason.value)
        logger.warning(
            "kg.rebuild.run_finalised run_id=%s board=%s actor=%s "
            "operation=%s outcome=%s reason=%s audit_ref=%s report_ref=%s",
            run_id, board_id, actor_id, operation, outcome.value, reason.value,
            audit_path, report_ref,
        )

        return RebuildRunResult(
            run_id=run_id,
            outcome=outcome.value,
            reason=reason.value,
            audit_ref=str(audit_path),
            previous_kg_generation_id=previous_kg_generation_id,
            current_kg_generation_id=current_kg_generation_id,
            started_at=started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            affected_files=affected_files,
            report_ref=report_ref,
            report_id=report_id,
            publishable_status=publishable_status,
            promotion_outcome=promotion_outcome,
            operator_action=operator_action,
            event_emitted=event_emitted,
        )


def _candidate_terminal_status_for(outcome: RebuildOutcome) -> str | None:
    """Map an internal RebuildOutcome to the api_1b43931d candidate
    ``terminal status``. Returns None for non-terminal outcomes that
    never publish a health state."""

    if outcome is RebuildOutcome.COMPLETED:
        return "completed"
    if outcome is RebuildOutcome.FAILED:
        return "rebuild_failed"
    if outcome is RebuildOutcome.REBUILD_FAILED:
        return "rebuild_failed"
    if outcome is RebuildOutcome.FAILED_ORPHAN_VALIDATION:
        return "failed_orphan_validation"
    return None


__all__ = [
    "AUDIT_DIRNAME",
    "KGRebuildService",
    "KGRebuiltEventEmitter",
    "OrphanScanProvider",
    "REBUILD_DIRNAME",
    "RebuildBlockReason",
    "RebuildOutcome",
    "RebuildRunResult",
    "RebuildStepAdapter",
    "RebuildStepInput",
    "RebuildStepResult",
    "SUPPORTED_REBUILD_OPERATIONS",
    "get_rebuild_run_count",
    "get_rebuild_run_counter_labels",
    "get_rebuild_run_samples",
    "reset_rebuild_run_counter",
]
