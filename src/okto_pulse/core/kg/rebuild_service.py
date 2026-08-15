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

import hashlib
import logging
import threading
import time
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping, Sequence

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    confirmation_fingerprint,
    resolve_rebuild_audit_artifact_store,
)
from okto_pulse.core.runtime_context import runtime_lock, runtime_state

logger = logging.getLogger("okto_pulse.kg.rebuild_service")


REBUILD_DIRNAME = "rebuild"
AUDIT_DIRNAME = "audit"
MAX_REBUILD_CONFIRMATION_RECEIPT_BYTES = 128 * 1024
REBUILD_CONFIRMATION_RECEIPT_SCHEMA = "kg_rebuild_confirmation_receipt.v1"


class RebuildConfirmationReceiptIntegrityError(RuntimeError):
    """The active recovery receipt exists but fails its run-bound contract."""


class ClosedRebuildReconciliation(str, Enum):
    """Bounded Core classification for a closed recovery operation.

    Community still proves the physical SQLite, quarantine, graph and
    generation state.  This enum prevents those adapters from inventing the
    semantic checkpoint/receipt rules that decide whether receipt-only archive
    or a fully compensated fresh run is even eligible.
    """

    RECEIPT_ONLY = "receipt_only"
    FULLY_COMPENSATED = "fully_compensated"
    AMBIGUOUS = "ambiguous"


def _is_confirmation_ref(value: object) -> bool:
    normalized = str(value or "").casefold()
    digest = normalized.removeprefix("conf_fp_")
    return (
        normalized.startswith("conf_fp_")
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
    )


def is_rebuild_terminal_audit_frozen(audit: Mapping[str, Any] | None) -> bool:
    """Return whether resume can safely return without any terminal effect."""

    if audit is None:
        return False
    return bool(
        audit.get("outcome") == RebuildOutcome.COMPLETED.value
        and audit.get("reason") == RebuildBlockReason.OK.value
        and bool(audit.get("report_ref"))
        and bool(audit.get("report_id"))
        and audit.get("publishable_status") == RebuildOutcome.COMPLETED.value
        and audit.get("promotion_outcome") == "promoted"
        and bool(audit.get("current_kg_generation_id"))
        and audit.get("event_emitted") is True
        and not audit.get("operator_action")
    )


_RESUMABLE_TERMINAL_ACTIONS = frozenset(
    {
        "reacquire_writer_and_resume",
        "resume_terminal_finalisation",
        "emit_terminal_event",
        "retry_report_persist",
        "retry_generation_promotion",
    }
)


def is_rebuild_terminal_audit_resumable(
    audit: Mapping[str, Any] | None,
) -> bool:
    """Classify bounded retry states that may re-enter the same run journal."""

    if audit is None:
        return True
    explicit = audit.get("same_run_resume_allowed")
    if isinstance(explicit, bool):
        return explicit
    return bool(
        audit.get("reason") == RebuildBlockReason.LEASE_LOST.value
        or audit.get("operator_action") in _RESUMABLE_TERMINAL_ACTIONS
    )


def is_rebuild_terminal_audit_closed(audit: Mapping[str, Any] | None) -> bool:
    """Return whether an active receipt may be rotated to a fresh operation."""

    if audit is None or is_rebuild_terminal_audit_resumable(audit):
        return False
    return str(audit.get("outcome") or "") in {
        RebuildOutcome.COMPLETED.value,
        RebuildOutcome.FAILED.value,
        RebuildOutcome.REBUILD_FAILED.value,
        RebuildOutcome.MANIFEST_DRIFT.value,
        RebuildOutcome.REPORT_PERSIST_FAILED.value,
        RebuildOutcome.FAILED_ORPHAN_VALIDATION.value,
    }


def classify_closed_rebuild_reconciliation(
    *,
    receipt: Mapping[str, Any] | None,
    audit: Mapping[str, Any] | None,
    checkpoint: Mapping[str, Any] | None,
) -> ClosedRebuildReconciliation:
    """Classify closed-operation state without inspecting edition storage.

    ``RECEIPT_ONLY`` is deliberately narrow: the audit is a bound, closed
    non-success with no terminal materialisation evidence, no affected files,
    and no F06 checkpoint. ``FULLY_COMPENSATED`` requires an exactly-bound
    failed checkpoint plus its successful, action-bound compensation receipt.
    Everything else is ambiguous and must not be archived or adopted by a
    fresh run without stronger reconciliation.
    """

    if receipt is None or audit is None:
        return ClosedRebuildReconciliation.AMBIGUOUS
    board_id = str(receipt.get("board_id") or "")
    if (
        receipt.get("receipt_state") not in (None, "authorized", "terminal")
        or not is_rebuild_confirmation_receipt_valid(receipt, board_id=board_id)
        or not is_rebuild_terminal_audit_closed(audit)
        or is_rebuild_terminal_audit_frozen(audit)
    ):
        return ClosedRebuildReconciliation.AMBIGUOUS
    if any(
        (
            audit.get("run_id") != receipt.get("run_id"),
            audit.get("board_id") != receipt.get("board_id"),
            audit.get("actor_id") != receipt.get("actor_id"),
            audit.get("operation") != receipt.get("operation"),
            audit.get("manifest_ref") != receipt.get("manifest_ref"),
            audit.get("user_reason") != receipt.get("user_reason"),
            audit.get("confirmation_ref") != receipt.get("confirmation_ref"),
            audit.get("report_ref"),
            audit.get("report_id"),
            audit.get("current_kg_generation_id"),
            audit.get("event_emitted") is True,
            audit.get("promotion_outcome") == "promoted",
            audit.get("publishable_status") == RebuildOutcome.COMPLETED.value,
        )
    ):
        return ClosedRebuildReconciliation.AMBIGUOUS
    if checkpoint is None:
        return (
            ClosedRebuildReconciliation.RECEIPT_ONLY
            if not tuple(audit.get("affected_files") or ())
            else ClosedRebuildReconciliation.AMBIGUOUS
        )
    if not isinstance(checkpoint, Mapping):
        return ClosedRebuildReconciliation.AMBIGUOUS
    command = checkpoint.get("command")
    receipts = checkpoint.get("receipts")
    if not isinstance(command, Mapping) or not isinstance(receipts, Mapping):
        return ClosedRebuildReconciliation.AMBIGUOUS

    def _value(raw: object) -> str:
        value = getattr(raw, "value", raw)
        return str(value or "")

    manifest_ref = str(receipt.get("manifest_ref") or "")
    f06_run_id = f"f06:{manifest_ref}"
    if any(
        (
            _value(checkpoint.get("state")) != "failed",
            command.get("run_id") != f06_run_id,
            command.get("board_id") != receipt.get("board_id"),
            command.get("manifest_ref") != manifest_ref,
            command.get("operation") != receipt.get("operation"),
            command.get("actor_id") != receipt.get("actor_id"),
            command.get("reason") != receipt.get("user_reason"),
        )
    ):
        return ClosedRebuildReconciliation.AMBIGUOUS
    compensation_actions = tuple(
        _value(action) for action in checkpoint.get("compensation_actions") or ()
    )
    compensation_key = f"{f06_run_id}:compensate"
    compensation = receipts.get(compensation_key)
    if not isinstance(compensation, Mapping):
        return ClosedRebuildReconciliation.AMBIGUOUS
    details = compensation.get("details")
    observed_actions = (
        tuple(_value(action) for action in details.get("actions") or ())
        if isinstance(details, Mapping)
        else ()
    )
    if any(
        (
            compensation.get("effect_key") != compensation_key,
            compensation.get("effect") != "compensate",
            compensation.get("ok") is not True,
            compensation.get("code") != "compensated",
            not isinstance(details, Mapping),
            observed_actions != compensation_actions,
        )
    ):
        return ClosedRebuildReconciliation.AMBIGUOUS
    return ClosedRebuildReconciliation.FULLY_COMPENSATED


def rebuild_operation_run_id(
    *,
    board_id: str,
    operation: str,
    preflight_hash: str,
    source_set_hash: str,
    manifest_ref: str,
) -> str:
    """Return the recoverable identity for one manifest-bound operation."""

    binding = "\x1f".join(
        (board_id, operation, preflight_hash, source_set_hash, manifest_ref)
    ).encode("utf-8")
    return f"run_{hashlib.sha256(binding).hexdigest()[:24]}"


def is_rebuild_confirmation_receipt_valid(
    receipt: Mapping[str, Any] | None,
    *,
    board_id: str,
    receipt_state: str | None = None,
) -> bool:
    """Validate the complete manifest-bound authorization receipt identity."""

    if receipt is None:
        return False
    required_strings = (
        "run_id",
        "board_id",
        "actor_id",
        "operation",
        "preflight_hash",
        "manifest_ref",
        "source_set_hash",
        "confirmation_ref",
        "user_reason",
        "started_at",
    )
    if receipt.get("schema_version") != REBUILD_CONFIRMATION_RECEIPT_SCHEMA:
        return False
    if any(
        not isinstance(receipt.get(key), str) or not receipt.get(key)
        for key in required_strings
    ):
        return False
    if receipt.get("board_id") != board_id:
        return False
    if not _is_confirmation_ref(receipt.get("confirmation_ref")):
        return False
    if receipt_state is not None and receipt.get("receipt_state") != receipt_state:
        return False
    try:
        started_at = datetime.fromisoformat(str(receipt["started_at"]))
    except (TypeError, ValueError):
        return False
    if started_at.tzinfo is None:
        return False
    expected_run_id = rebuild_operation_run_id(
        board_id=board_id,
        operation=str(receipt["operation"]),
        preflight_hash=str(receipt["preflight_hash"]),
        source_set_hash=str(receipt["source_set_hash"]),
        manifest_ref=str(receipt["manifest_ref"]),
    )
    return receipt.get("run_id") == expected_run_id


def rebuild_confirmation_receipt_key(
    *,
    board_id: str,
    run_id: str,
) -> RebuildAuditKey:
    """Return the board-scoped durable authorization receipt key."""

    return RebuildAuditKey(
        namespace="rebuild_confirmation_receipt",
        board_id=board_id,
        artifact_id=run_id,
    )


def rebuild_active_confirmation_receipt_key(*, board_id: str) -> RebuildAuditKey:
    """Return the O(1) active-operation pointer for one board."""

    return RebuildAuditKey(
        namespace="rebuild_confirmation_receipt",
        board_id=board_id,
        artifact_id="active",
    )


def list_rebuild_confirmation_receipts(
    *,
    artifact_store: RebuildAuditArtifactStore,
    board_id: str,
) -> tuple[dict[str, Any], ...]:
    """List bounded receipt candidates for the isolated recovery executor."""

    rows = artifact_store.list_json_bounded(
        rebuild_active_confirmation_receipt_key(board_id=board_id),
        max_results=1,
        max_document_bytes=MAX_REBUILD_CONFIRMATION_RECEIPT_BYTES,
    )
    return tuple(dict(row) for row in rows)


def load_verified_rebuild_confirmation_receipt(
    *,
    artifact_store: RebuildAuditArtifactStore,
    board_id: str,
) -> dict[str, Any] | None:
    """Load the O(1) active receipt and verify its recoverable identity.

    The recovery-only executor deliberately consumes this helper instead of
    reimplementing receipt shape or run-id derivation.  Journal extensions are
    tolerated, while the schema, board, canonical confirmation fingerprint,
    manifest binding, timeline, and deterministic run identity remain closed.
    """

    try:
        rows = artifact_store.list_json_bounded(
            rebuild_active_confirmation_receipt_key(board_id=board_id),
            max_results=1,
            max_document_bytes=MAX_REBUILD_CONFIRMATION_RECEIPT_BYTES,
        )
    except Exception as exc:
        raise RebuildConfirmationReceiptIntegrityError(
            "rebuild_confirmation_active_receipt_read_failed"
        ) from exc
    if not rows:
        # ``active`` is the deletion sentinel for this namespace.  Physical
        # erasure removes every history first and active last, so a history
        # without active is evidence of a torn/foreign purge, never a clean
        # fresh-recovery state.  Keep this bounded: any row or any inability to
        # prove absence fails closed, while unrelated legacy namespaces remain
        # outside this oracle.
        try:
            orphaned_history = artifact_store.list_json_bounded(
                RebuildAuditKey(
                    namespace="rebuild_confirmation_receipt",
                    board_id=board_id,
                ),
                max_results=1,
                max_document_bytes=MAX_REBUILD_CONFIRMATION_RECEIPT_BYTES,
            )
        except Exception as exc:
            raise RebuildConfirmationReceiptIntegrityError(
                "rebuild_confirmation_active_missing_history_unverifiable"
            ) from exc
        if orphaned_history:
            raise RebuildConfirmationReceiptIntegrityError(
                "rebuild_confirmation_active_missing_with_history"
            )
        return None
    if len(rows) != 1:
        raise RebuildConfirmationReceiptIntegrityError(
            "rebuild_confirmation_active_receipt_cardinality_invalid"
        )
    receipt = dict(rows[0])
    receipt_state = receipt.get("receipt_state")
    if receipt_state not in (None, "authorized", "terminal"):
        raise RebuildConfirmationReceiptIntegrityError(
            "rebuild_confirmation_active_receipt_state_invalid"
        )
    if not is_rebuild_confirmation_receipt_valid(receipt, board_id=board_id):
        raise RebuildConfirmationReceiptIntegrityError(
            "rebuild_confirmation_active_receipt_integrity_invalid"
        )
    # Archive order is history -> active. A terminal active marker requires
    # the exact history witness. An authorized active may have no history, or
    # the exact terminalized form when a crash happened between history commit
    # and active CAS; any other same-run history is conflict evidence that must
    # fail before the executor can mutate again.
    try:
        history_rows = artifact_store.list_json_bounded(
            rebuild_confirmation_receipt_key(
                board_id=board_id,
                run_id=str(receipt["run_id"]),
            ),
            max_results=1,
            max_document_bytes=MAX_REBUILD_CONFIRMATION_RECEIPT_BYTES,
        )
    except Exception as exc:
        if receipt_state == "terminal":
            raise RebuildConfirmationReceiptIntegrityError(
                "rebuild_confirmation_terminal_history_unverifiable"
            ) from exc
        raise RebuildConfirmationReceiptIntegrityError(
            "rebuild_confirmation_authorized_history_unverifiable"
        ) from exc
    if receipt_state == "terminal":
        if len(history_rows) != 1 or dict(history_rows[0]) != receipt:
            raise RebuildConfirmationReceiptIntegrityError(
                "rebuild_confirmation_terminal_history_mismatch"
            )
    elif history_rows:
        expected_terminal = {**receipt, "receipt_state": "terminal"}
        if len(history_rows) != 1 or dict(history_rows[0]) != expected_terminal:
            raise RebuildConfirmationReceiptIntegrityError(
                "rebuild_confirmation_authorized_history_mismatch"
            )
        try:
            audit_rows = artifact_store.list_json_bounded(
                RebuildAuditKey(
                    namespace="run_audit",
                    board_id=board_id,
                    artifact_id=str(receipt["run_id"]),
                ),
                max_results=1,
                max_document_bytes=MAX_REBUILD_CONFIRMATION_RECEIPT_BYTES,
            )
        except Exception as exc:
            raise RebuildConfirmationReceiptIntegrityError(
                "rebuild_confirmation_authorized_history_audit_unverifiable"
            ) from exc
        audit = dict(audit_rows[0]) if len(audit_rows) == 1 else None
        if (
            not is_rebuild_terminal_audit_closed(audit)
            or audit.get("run_id") != receipt.get("run_id")
            or audit.get("board_id") != receipt.get("board_id")
            or audit.get("actor_id") != receipt.get("actor_id")
            or audit.get("operation") != receipt.get("operation")
            or audit.get("manifest_ref") != receipt.get("manifest_ref")
            or audit.get("user_reason") != receipt.get("user_reason")
            or audit.get("confirmation_ref") != receipt.get("confirmation_ref")
        ):
            raise RebuildConfirmationReceiptIntegrityError(
                "rebuild_confirmation_authorized_history_audit_mismatch"
            )
    return receipt


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
    # Rebuild execution is intentionally unavailable from normal REST/MCP
    # runtime composition until a board-wide relational writer fence exists.
    RECOVERY_EXECUTION_REQUIRED = "recovery_execution_required"


class RebuildBlockReason(str, Enum):
    """Bounded reason vocabulary for OR or_37cebd03 ``reason`` label."""

    CONFIRMATION_INVALID = "confirmation_invalid"
    MANIFEST_NOT_FOUND = "manifest_not_found"
    MANIFEST_DRIFT = "manifest_drift"
    LOCK_CONTENTION = "lock_contention"
    LEASE_LOST = "lease_lost"
    LIFECYCLE_FAILED = "lifecycle_failed"
    STEP_EXCEPTION = "step_exception"
    QUARANTINE_FAILED = "quarantine_failed"
    OPERATION_PENDING_KG02_4 = "operation_pending_kg02_4"
    # KG-02.4 — report-first terminal gate reasons.
    REPORT_PERSIST_STORE_FAILED = "report_persist_store_failed"
    REPORT_PERSIST_SENSITIVE_REJECTED = "report_persist_sensitive_rejected"
    GENERATION_PROMOTION_BLOCKED = "generation_promotion_blocked"
    GENERATION_STORE_UNAVAILABLE = "generation_store_unavailable"
    ORPHAN_VALIDATION_FAILED = "orphan_validation_failed"
    RECOVERY_EXECUTION_REQUIRED = "recovery_execution_required"
    # SPEC4 card 619e58e1 (G1): the rebuild consumed sources but materialized an
    # empty graph — partition materialization failed, refuse to promote.
    MATERIALIZED_LAYER_MISMATCH = "materialized_layer_mismatch"
    OK = "ok"


# val_dfdff0b8 rework: KG-02.3 fully implements ONLY the ``rebuild``
# operation. reset/quarantine/promote/rollback/reindex_discovery land
# in KG-02.4 (generation repo + report-first) and KG-02.6 (discovery
# reindex). Until then ANY non-rebuild operation must short-circuit
# with UNSUPPORTED_OPERATION BEFORE the lock is taken so we never
# emit a silent "completed" for a destructive op.
SUPPORTED_REBUILD_OPERATIONS: frozenset[str] = frozenset({"rebuild"})


def _materialized_layer_counts(board_id: str) -> dict[str, int]:
    """G1 (SPEC4 card 619e58e1): bounded read-only per-``graph_layer`` node count
    of the REAL board graph.

    The deterministic structural materialiser is an identity placeholder, so the
    materialized graph is the only authoritative per-partition state. Mirrors
    ``kg_health._aggregate_kg_layer_counts``' safe per-NODE_TYPE pattern (one
    ``MATCH (n:Label)`` per type — never the unsupported generic ``MATCH (n)``).
    Called by the orchestrator AFTER the safe-write lifecycle (checkpoint/flush/
    fsync/close-reopen probe), so opening the graph here cannot interfere with
    that durability gate. Degrades to ``{}`` on a graph backend/schema error.
    """
    counts: dict[str, int] = {}
    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry
        from okto_pulse.core.kg.schema_contract import NODE_TYPES

        cypher = get_kg_registry().cypher_executor
        for node_type in NODE_TYPES:
            try:
                result = cypher.execute_read_only(
                    board_id,
                    f"MATCH (n:{node_type}) RETURN n.graph_layer, count(n)",
                    max_rows=10000,
                )
                for row in result.get("rows", []):
                    layer = str(row[0] or "unclassified")
                    counts[layer] = counts.get(layer, 0) + int(row[1] or 0)
            except Exception:
                continue
    except Exception as exc:
        logger.warning(
            "kg.rebuild.materialized_layer_probe_failed board=%s err=%s",
            board_id,
            exc,
        )
        return {}
    return counts


def _verify_materialized_layers(
    board_id: str, step_result: RebuildStepResult
) -> tuple[str, dict[str, int]] | None:
    """G1 (SPEC4 card 619e58e1): partition-aware materialization guard.

    Every graph layer the resolved source set expected to materialize
    (``expected_by_layer[layer] > 0``, produced deterministically by the adapter)
    MUST be present in the REAL board graph (queried here, AFTER safe-write).
    source->node is NOT 1:1, so this is a PRESENCE check, not exact equality;
    both maps are returned for audit. ``empty_after_materialized`` is the subcase
    where every expected layer is missing (e.g. canonical sources resolved but
    the canonical partition is empty after the rebuild). Returns
    ``(detail, materialized)`` when the guard trips, else ``None``. Never
    promotes a mismatch.
    """
    counts = getattr(step_result, "counts", None) or {}
    expected = counts.get("expected_by_layer") or {}
    if not expected:
        # No source partition resolved → no materialization claim to verify.
        return None
    materialized = _materialized_layer_counts(board_id)
    missing = sorted(
        layer
        for layer, n in expected.items()
        if int(n or 0) > 0 and int(materialized.get(layer, 0) or 0) == 0
    )
    if missing:
        detail = (
            "materialized_layer_mismatch: expected partitions "
            f"{dict(expected)} but materialized {dict(materialized)} — "
            f"missing/empty layer(s) {missing}. Refusing to promote a rebuild "
            "that did not materialize an expected partition "
            "(empty_after_materialized guard)."
        )
        return detail, materialized
    return None


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
_rebuild_counter = runtime_state("kg.rebuild_service.counter", dict)
_rebuild_counter_lock = runtime_lock("kg.rebuild_service.counter")


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
    # Cooperative controls supplied by the outer orchestrator.  Edition
    # adapters poll these while executing long-running rebuild phases so an
    # MCP cancellation cannot leave a background thread holding the writer
    # lease, and the lease cannot expire during a valid long drain.
    cancel_requested: Callable[[], bool] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    lease_renew: Callable[[], bool] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    orchestration_renew: Callable[[], bool] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    release_writer_for_drain: Callable[[], bool] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    reacquire_writer_after_drain: Callable[[], str | None] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    source_revalidate: Callable[[], bool] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    # Recovery-only compensation lane. When set, the edition adapter must
    # load the existing durable checkpoint and call ``fail_existing`` without
    # resolving live sources or starting any fresh rebuild effect.
    recovery_failure_code: str | None = None
    recovery_failure_detail: str | None = None
    # A proved v1/v2 -> current-schema rebaseline must materialize the fresh
    # live projection whose compatibility hash was checked, never the legacy
    # rows persisted in the old manifest. Both fields are set together only
    # after the run-bound rebaseline evidence is durably recorded under R+A.
    rebaseline_source_rows: tuple[Mapping[str, Any], ...] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    rebaseline_evidence_id: str | None = None
    rebaseline_target_source_set_hash: str | None = None


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
    reconciliation_decisions: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    drilldown: dict[str, Any] = field(default_factory=dict)
    rebaseline_evidence_id: str | None = None
    rebaseline_target_source_set_hash: str | None = None


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
KGRebuiltEventEmitter = Callable[[dict[str, Any]], Any]
OrphanScanProvider = Callable[[str, str | None], Any]


def _default_event_emitter(_event: dict[str, Any]) -> bool:
    # Recovery-capable composition must provide the real durable publisher.
    # Treat an omitted adapter as a retryable terminal phase, never as proof
    # that ``kg.rebuilt`` was delivered.
    return False


class _RebuildLeaseHeartbeat:
    """Renew one exact single-writer token while blocking rebuild code runs."""

    def __init__(
        self,
        renew: Callable[[], bool],
        *,
        board_id: str,
        interval_seconds: float,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("lease heartbeat interval must be positive")
        self._renew = renew
        self._board_id = board_id
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._renew_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"kg-rebuild-lease:{self._board_id}",
            daemon=True,
        )
        self._thread.start()

    def renew_now(self) -> bool:
        if self._lost.is_set():
            return False
        with self._renew_lock:
            if self._lost.is_set():
                return False
            try:
                renewed = bool(self._renew())
            except Exception as exc:
                logger.error(
                    "kg.rebuild.lease_renew_failed board=%s err=%s",
                    self._board_id,
                    exc,
                )
                renewed = False
            if not renewed:
                self._lost.set()
            return renewed

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self._interval_seconds * 2))

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            if not self.renew_now():
                return


class _TerminalFenceLost(RuntimeError):
    """A terminal side effect was fenced after the rebuild step completed."""

    def __init__(
        self,
        detail: str,
        *,
        durable_decision: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(detail)
        self.durable_decision = dict(durable_decision or {})


# --- The service -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class KGRebuildService:
    """Orchestrator that wraps a rebuild step in the KG-01 admin lane.

    Adapters are injected: tests pass fakes for the lock, quarantine,
    confirmation store, manifest store and step. Production wires the
    real KG-01.1-5 primitives.
    """

    base_dir: object | None
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
    lease_heartbeat_interval_seconds: float | None = None
    lease_reacquire_timeout_seconds: float = 5.0
    lease_reacquire_poll_interval_seconds: float = 0.05
    operation_reservation: Any | None = None
    artifact_store: RebuildAuditArtifactStore | None = None

    def __post_init__(self) -> None:
        if (
            self.lease_heartbeat_interval_seconds is not None
            and self.lease_heartbeat_interval_seconds <= 0
        ):
            raise ValueError("lease_heartbeat_interval_seconds must be positive")
        if self.lease_reacquire_timeout_seconds <= 0:
            raise ValueError("lease_reacquire_timeout_seconds must be positive")
        if self.lease_reacquire_poll_interval_seconds <= 0:
            raise ValueError("lease_reacquire_poll_interval_seconds must be positive")
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    # --- public API --------------------------------------------------------

    def resume_authorized_run(
        self,
        *,
        run_id: str,
        board_id: str,
        actor_id: str,
        operation: str,
        preflight_hash: str,
        manifest_ref: str,
        reason: str,
        cancel_requested: Callable[[], bool] | None = None,
        recovery_capability: object | None = None,
    ) -> RebuildRunResult:
        """Resume one receipt-authorized, non-terminal offline operation.

        This API is intentionally in-process only. REST/MCP never compose it.
        It requires the same opaque recovery capability as a fresh run and
        revalidates the exact durable receipt/manifest binding under the board's
        administrative reservation. The raw confirmation token is not needed.
        """

        return self.run(
            confirmation_id="receipt_authorized_resume",
            board_id=board_id,
            actor_id=actor_id,
            operation=operation,
            preflight_hash=preflight_hash,
            manifest_ref=manifest_ref,
            reason=reason,
            cancel_requested=cancel_requested,
            recovery_capability=recovery_capability,
            _resume_run_id=run_id,
        )

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
        cancel_requested: Callable[[], bool] | None = None,
        recovery_capability: object | None = None,
        _resume_run_id: str | None = None,
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
        from okto_pulse.core.kg.recovery_execution import (
            check_recovery_execution_capability_scope,
            validate_recovery_execution_capability,
        )
        from okto_pulse.core.kg.write_barrier import under_safe_write

        confirmation_ref = confirmation_fingerprint(confirmation_id)
        # Admission denials happen before a source manifest is trusted. This
        # temporary id is never persisted board-scoped; a recoverable operation
        # id is derived from the manifest/source-set binding under reservation.
        run_id = f"admission_{confirmation_ref[:24]}"
        operation_run_id: str | None = None
        started_at = datetime.now(timezone.utc)

        def _recovery_scope_valid() -> bool:
            return check_recovery_execution_capability_scope(
                recovery_capability,
                board_id=board_id,
            )

        def _recovery_execution_valid() -> bool:
            if operation_run_id is None:
                return _recovery_scope_valid()
            return validate_recovery_execution_capability(
                recovery_capability,
                board_id=board_id,
                run_id=operation_run_id,
            )

        # Normal REST/MCP callers cannot mint this opaque in-process authority.
        # Reject before the one-shot confirmation is consumed so the dedicated
        # offline runner can still execute the confirmed operation.
        if not _recovery_scope_valid():
            return self._emit_audit_and_counter(
                run_id=run_id,
                outcome=RebuildOutcome.RECOVERY_EXECUTION_REQUIRED,
                reason=RebuildBlockReason.RECOVERY_EXECUTION_REQUIRED,
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
                    "offline recovery execution capability missing, expired, "
                    "or board-mismatched"
                ),
            )

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

        if not _recovery_execution_valid():
            return self._emit_audit_and_counter(
                run_id=run_id,
                outcome=RebuildOutcome.RECOVERY_EXECUTION_REQUIRED,
                reason=RebuildBlockReason.RECOVERY_EXECUTION_REQUIRED,
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
                detail="offline recovery execution capability expired before consume",
            )

        if self.source_enumerator is None:
            return self._emit_audit_and_counter(
                run_id=run_id,
                outcome=RebuildOutcome.REBUILD_FAILED,
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
                detail="source_enumerator_required_for_recovery_execution",
            )

        # Reserve administrative ownership before creating any board-scoped
        # receipt. This shares the same fence as erasure, so contention cannot
        # recreate board artifacts after a concurrent purge.
        from okto_pulse.core.kg.single_writer_lock import (
            KGAdministrativeOperationReservation,
        )

        bind_write_lock_port = getattr(
            self.single_writer_lock,
            "bind_write_lock_port",
            None,
        )
        bound_write_lock_port = (
            bind_write_lock_port() if callable(bind_write_lock_port) else None
        )
        operation_reservation = self.operation_reservation
        if operation_reservation is None:
            operation_reservation = KGAdministrativeOperationReservation(
                base_dir=self.base_dir,
                write_lock_port=bound_write_lock_port,
            )
        bind_reservation_port = getattr(
            operation_reservation,
            "bind_write_lock_port",
            None,
        )
        if callable(bind_reservation_port):
            bind_reservation_port()

        reservation_acquisition = operation_reservation.acquire(
            board_id=board_id,
            operation=f"kg02_rebuild_reservation:{manifest_ref}",
            owner_id=actor_id,
            ttl_seconds=self.lock_ttl_seconds,
            admin_lane=True,
        )
        if (
            not reservation_acquisition.acquired
            or not reservation_acquisition.owner_token
        ):
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
                detail=(
                    "administrative_reservation_owner="
                    f"{reservation_acquisition.current_owner}"
                ),
            )

        reservation_token = reservation_acquisition.owner_token
        heartbeat_interval_seconds = (
            self.lease_heartbeat_interval_seconds
            if self.lease_heartbeat_interval_seconds is not None
            else max(0.1, min(30.0, self.lock_ttl_seconds / 3))
        )
        reservation_heartbeat: _RebuildLeaseHeartbeat | None = None
        try:
            reservation_heartbeat = _RebuildLeaseHeartbeat(
                lambda: (
                    _recovery_execution_valid()
                    and operation_reservation.renew(
                        board_id=board_id,
                        owner_token=reservation_token,
                        ttl_seconds=self.lock_ttl_seconds,
                    )
                ),
                board_id=board_id,
                interval_seconds=heartbeat_interval_seconds,
            )
            reservation_heartbeat.start()
        except BaseException:
            if reservation_heartbeat is not None:
                try:
                    reservation_heartbeat.stop()
                except BaseException:
                    logger.exception(
                        "kg.rebuild.reservation_setup_heartbeat_stop_failed board=%s",
                        board_id,
                    )
            try:
                operation_reservation.release(
                    board_id=board_id,
                    owner_token=reservation_token,
                )
            except BaseException:
                logger.exception(
                    "kg.rebuild.reservation_setup_release_failed board=%s",
                    board_id,
                )
            raise
        assert reservation_heartbeat is not None

        def _cleanup_initial_reservation() -> None:
            try:
                reservation_heartbeat.stop()
            except BaseException:
                logger.exception(
                    "kg.rebuild.initial_reservation_heartbeat_stop_failed board=%s",
                    board_id,
                )
            try:
                operation_reservation.release(
                    board_id=board_id,
                    owner_token=reservation_token,
                )
            except BaseException:
                logger.exception(
                    "kg.rebuild.initial_reservation_release_failed board=%s",
                    board_id,
                )

        # Acquire graph writer A before loading/revalidating any source
        # manifest and before consuming the one-shot confirmation. This is the
        # linearization fence against an ordinary worker commit at admission.
        initial_acquire_deadline = (
            time.monotonic() + self.lease_reacquire_timeout_seconds
        )
        acquisition = None
        try:
            while True:
                if not (
                    _recovery_execution_valid() and reservation_heartbeat.renew_now()
                ):
                    try:
                        return self._emit_audit_and_counter(
                            run_id=run_id,
                            outcome=RebuildOutcome.REBUILD_FAILED,
                            reason=RebuildBlockReason.LEASE_LOST,
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
                                "administrative reservation lost before writer acquire"
                            ),
                        )
                    finally:
                        _cleanup_initial_reservation()
                acquisition = self.single_writer_lock.acquire(
                    board_id=board_id,
                    operation=f"kg02_rebuild:{operation}",
                    owner_id=actor_id,
                    ttl_seconds=self.lock_ttl_seconds,
                    admin_lane=True,
                )
                if acquisition.acquired and acquisition.owner_token:
                    break
                remaining = initial_acquire_deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(self.lease_reacquire_poll_interval_seconds, remaining))
        except BaseException:
            _cleanup_initial_reservation()
            raise
        assert acquisition is not None
        if not acquisition.acquired or not acquisition.owner_token:
            try:
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
            finally:
                _cleanup_initial_reservation()

        owner_token = acquisition.owner_token

        def _cleanup_initial_ownership() -> None:
            try:
                self.single_writer_lock.release(
                    board_id=board_id,
                    owner_token=owner_token,
                )
            except BaseException:
                logger.exception(
                    "kg.rebuild.initial_writer_release_failed board=%s",
                    board_id,
                )
            _cleanup_initial_reservation()

        confirmation_receipt_key = rebuild_active_confirmation_receipt_key(
            board_id=board_id,
        )
        resume_receipt: Mapping[str, Any] | None = None
        recovery_failure_code: str | None = None
        recovery_failure_detail: str | None = None
        authorized_source_set_hash: str | None = None

        # Resume authorization is the durable authority when the immutable
        # manifest can no longer be loaded/revalidated. Read and bind it under
        # reservation + writer A before touching the manifest so drift can
        # drive compensation instead of stranding a non-terminal receipt.
        if _resume_run_id is not None:
            try:
                resume_receipt = self.confirmation_store.artifact_store.read_json(
                    confirmation_receipt_key
                )
                if resume_receipt is None:
                    resume_receipt = self.confirmation_store.artifact_store.read_json(
                        rebuild_confirmation_receipt_key(
                            board_id=board_id,
                            run_id=_resume_run_id,
                        )
                    )
            except BaseException:
                _cleanup_initial_ownership()
                raise
            receipt_valid = bool(
                is_rebuild_confirmation_receipt_valid(
                    resume_receipt,
                    board_id=board_id,
                )
                and resume_receipt is not None
                and resume_receipt.get("run_id") == _resume_run_id
                and resume_receipt.get("actor_id") == actor_id
                and resume_receipt.get("operation") == operation
                and resume_receipt.get("preflight_hash") == preflight_hash
                and resume_receipt.get("manifest_ref") == manifest_ref
                and resume_receipt.get("user_reason") == reason
            )
            if not receipt_valid:
                try:
                    return self._emit_audit_and_counter(
                        run_id=_resume_run_id,
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
                        detail="authorized_resume_receipt_invalid",
                    )
                finally:
                    _cleanup_initial_ownership()
            assert resume_receipt is not None
            operation_run_id = _resume_run_id
            run_id = _resume_run_id
            authorized_source_set_hash = str(resume_receipt["source_set_hash"])
            confirmation_ref = str(resume_receipt["confirmation_ref"])
            try:
                authorized_started_at = datetime.fromisoformat(
                    str(resume_receipt["started_at"])
                )
                if authorized_started_at.tzinfo is None:
                    raise ValueError("receipt started_at must be timezone-aware")
                started_at = authorized_started_at.astimezone(timezone.utc)
            except (TypeError, ValueError):
                _cleanup_initial_ownership()
                raise RuntimeError("authorized_resume_started_at_invalid") from None
            if not _recovery_execution_valid():
                try:
                    return self._emit_audit_and_counter(
                        run_id=run_id,
                        outcome=RebuildOutcome.REBUILD_FAILED,
                        reason=RebuildBlockReason.LEASE_LOST,
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
                        detail="offline recovery capability expired before resume",
                    )
                finally:
                    _cleanup_initial_ownership()

        # Load the immutable manifest under reservation + writer A before consuming
        # confirmation. Its content hash establishes a recoverable operation id
        # that a fresh offline process can derive without the raw token.
        manifest = None
        try:
            manifest = self.manifest_store.load(manifest_ref)
        except BaseException as exc:
            if resume_receipt is None:
                _cleanup_initial_ownership()
                raise
            recovery_failure_code = "manifest_drift"
            recovery_failure_detail = f"manifest_load_exception:{type(exc).__name__}"
        if manifest is None:
            if resume_receipt is not None:
                if recovery_failure_code is None:
                    recovery_failure_code = "manifest_drift"
                    recovery_failure_detail = "manifest_missing_during_resume"
            else:
                try:
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
                        detail="manifest missing before confirmation consume",
                    )
                finally:
                    _cleanup_initial_ownership()

        if manifest is not None:
            try:
                current_source_set = self.source_enumerator.enumerate(board_id=board_id)
                integrity_ok = self.manifest_store.validate_integrity(
                    manifest=manifest,
                    expected_manifest_ref=manifest_ref,
                    expected_board_id=board_id,
                    expected_preflight_hash=preflight_hash,
                    cognitive_durable_digest=(
                        current_source_set.cognitive_durable_digest
                    ),
                )
                preconsume_revalidation = self.manifest_store.classify_revalidation(
                    manifest=manifest,
                    current_source_set=current_source_set,
                )
            except BaseException as exc:
                if resume_receipt is None:
                    _cleanup_initial_ownership()
                    raise
                integrity_ok = False
                preconsume_revalidation = None
                recovery_failure_code = "manifest_drift"
                recovery_failure_detail = (
                    f"manifest_revalidation_exception:{type(exc).__name__}"
                )
            if not integrity_ok or (
                preconsume_revalidation is not None and preconsume_revalidation.is_drift
            ):
                if resume_receipt is not None:
                    recovery_failure_code = "manifest_drift"
                    recovery_failure_detail = (
                        "manifest_integrity_invalid"
                        if not integrity_ok
                        else "manifest_drift_during_resume"
                    )
                else:
                    try:
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
                            detail=(
                                "manifest_integrity_invalid"
                                if not integrity_ok
                                else "manifest_drift_before_confirmation_consume"
                            ),
                        )
                    finally:
                        _cleanup_initial_ownership()
            if authorized_source_set_hash is None:
                authorized_source_set_hash = manifest.source_set_hash

        if operation_run_id is None:
            assert manifest is not None and authorized_source_set_hash is not None
            operation_run_id = rebuild_operation_run_id(
                board_id=board_id,
                operation=operation,
                preflight_hash=preflight_hash,
                source_set_hash=authorized_source_set_hash,
                manifest_ref=manifest_ref,
            )
            run_id = operation_run_id
        if not (_recovery_execution_valid() and reservation_heartbeat.renew_now()):
            try:
                return self._emit_audit_and_counter(
                    run_id=run_id,
                    outcome=RebuildOutcome.RECOVERY_EXECUTION_REQUIRED,
                    reason=RebuildBlockReason.RECOVERY_EXECUTION_REQUIRED,
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
                    detail="offline recovery capability expired before consume",
                )
            finally:
                _cleanup_initial_ownership()

        # 1. Fresh execution atomically consumes its confirmation and creates
        # durable authorization proof. A separate in-process resume API can use
        # that proof without the raw token, but only for this exact operation id
        # and only while the opaque offline capability remains live.
        assert authorized_source_set_hash is not None
        confirmation_receipt = {
            "schema_version": REBUILD_CONFIRMATION_RECEIPT_SCHEMA,
            "run_id": run_id,
            "board_id": board_id,
            "actor_id": actor_id,
            "operation": operation,
            "preflight_hash": preflight_hash,
            "manifest_ref": manifest_ref,
            "source_set_hash": authorized_source_set_hash,
            "confirmation_ref": confirmation_ref,
            "user_reason": reason,
            "started_at": started_at.isoformat(),
        }
        try:
            existing_receipt = self.confirmation_store.artifact_store.read_json(
                confirmation_receipt_key
            )
            if existing_receipt is None and _resume_run_id is not None:
                existing_receipt = self.confirmation_store.artifact_store.read_json(
                    rebuild_confirmation_receipt_key(
                        board_id=board_id,
                        run_id=run_id,
                    )
                )
        except BaseException:
            _cleanup_initial_ownership()
            raise

        def _initial_fences_valid() -> bool:
            try:
                return bool(
                    _recovery_execution_valid()
                    and reservation_heartbeat.renew_now()
                    and self.single_writer_lock.renew(
                        board_id=board_id,
                        owner_token=owner_token,
                        ttl_seconds=self.lock_ttl_seconds,
                    )
                )
            except BaseException:
                logger.exception(
                    "kg.rebuild.initial_fence_probe_failed board=%s run=%s",
                    board_id,
                    run_id,
                )
                return False

        if not _initial_fences_valid():
            try:
                return self._emit_audit_and_counter(
                    run_id=run_id,
                    outcome=RebuildOutcome.REBUILD_FAILED,
                    reason=RebuildBlockReason.LEASE_LOST,
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
                    detail="administrative fences lost before confirmation consume",
                )
            finally:
                _cleanup_initial_ownership()

        authorized_confirmation_ref = confirmation_ref

        def _archive_confirmation_receipt(receipt: Mapping[str, Any]) -> None:
            history_key = rebuild_confirmation_receipt_key(
                board_id=board_id,
                run_id=run_id,
            )
            archive_payload = {
                **dict(receipt),
                "receipt_state": "terminal",
            }

            def _close_exact_history(
                current: dict[str, Any] | None,
            ) -> dict[str, Any]:
                # This callback executes under the artifact store's exclusive
                # transaction.  Proving R+B inside that boundary prevents an
                # expired writer that waited behind erasure from recreating the
                # history artifact after the purge completed.
                if not _initial_fences_valid():
                    raise RuntimeError(
                        "rebuild_confirmation_archive_fence_lost_before_history"
                    )
                if current not in (None, archive_payload):
                    raise RuntimeError("rebuild_confirmation_receipt_archive_conflict")
                return archive_payload

            self.confirmation_store.artifact_store.replace_json(
                history_key,
                _close_exact_history,
            )
            if not _initial_fences_valid():
                raise RuntimeError(
                    "rebuild_confirmation_archive_fence_lost_before_active"
                )

            expected_active = dict(receipt)

            def _close_exact_active(
                current: dict[str, Any] | None,
            ) -> dict[str, Any]:
                # Atomic compare-and-replace: erasure may have purged the key,
                # or a later operation may have installed a different active
                # receipt after our lease expired.  Neither case may be
                # recreated/overwritten by this old run.
                if not _initial_fences_valid():
                    raise RuntimeError(
                        "rebuild_confirmation_archive_fence_lost_during_active"
                    )
                if current != expected_active:
                    raise RuntimeError("rebuild_confirmation_active_receipt_conflict")
                return archive_payload

            self.confirmation_store.artifact_store.replace_json(
                confirmation_receipt_key,
                _close_exact_active,
            )

        def _terminal_receipt_is_closed(receipt: Mapping[str, Any]) -> bool:
            if not is_rebuild_confirmation_receipt_valid(
                receipt,
                board_id=board_id,
                receipt_state="terminal",
            ):
                return False
            receipt_run_id = str(receipt.get("run_id") or "")
            archived = self.confirmation_store.artifact_store.read_json(
                rebuild_confirmation_receipt_key(
                    board_id=board_id,
                    run_id=receipt_run_id,
                )
            )
            if archived != dict(receipt):
                return False
            audit = self.artifact_store.read_json(
                RebuildAuditKey(
                    namespace="run_audit",
                    board_id=board_id,
                    artifact_id=receipt_run_id,
                )
            )
            if not is_rebuild_terminal_audit_closed(audit):
                return False
            if any(
                (
                    audit.get("run_id") != receipt_run_id,
                    audit.get("board_id") != receipt.get("board_id"),
                    audit.get("actor_id") != receipt.get("actor_id"),
                    audit.get("operation") != receipt.get("operation"),
                    audit.get("manifest_ref") != receipt.get("manifest_ref"),
                    audit.get("user_reason") != receipt.get("user_reason"),
                    audit.get("confirmation_ref") != receipt.get("confirmation_ref"),
                )
            ):
                return False
            if not is_rebuild_terminal_audit_frozen(audit):
                return True
            return bool(
                self.generation_repository is not None
                and self.generation_repository.get_current(board_id)
                == audit.get("current_kg_generation_id")
            )

        if _resume_run_id is not None:
            receipt_binding = {
                key: confirmation_receipt[key]
                for key in (
                    "schema_version",
                    "run_id",
                    "board_id",
                    "actor_id",
                    "operation",
                    "preflight_hash",
                    "manifest_ref",
                    "source_set_hash",
                    "user_reason",
                )
            }
            observed_binding = (
                {key: existing_receipt.get(key) for key in receipt_binding}
                if existing_receipt is not None
                else None
            )
            receipt_valid = (
                bool(
                    _resume_run_id == run_id
                    and observed_binding == receipt_binding
                    and _is_confirmation_ref(existing_receipt.get("confirmation_ref"))
                    and isinstance(existing_receipt.get("started_at"), str)
                )
                if existing_receipt is not None
                else False
            )
            if not receipt_valid:
                try:
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
                        detail="authorized_resume_receipt_invalid",
                    )
                finally:
                    _cleanup_initial_ownership()

            authorized_confirmation_ref = str(existing_receipt["confirmation_ref"])
            try:
                authorized_started_at = datetime.fromisoformat(
                    str(existing_receipt["started_at"])
                )
                if authorized_started_at.tzinfo is None:
                    raise ValueError("receipt started_at must be timezone-aware")
                started_at = authorized_started_at.astimezone(timezone.utc)
            except (TypeError, ValueError):
                try:
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
                        detail="authorized_resume_started_at_invalid",
                    )
                finally:
                    _cleanup_initial_ownership()

            # A terminal run is immutable. Return its frozen decision without
            # repeating quarantine/report/promotion/event or counter effects.
            terminal_audit_key = RebuildAuditKey(
                namespace="run_audit",
                board_id=board_id,
                artifact_id=run_id,
            )
            try:
                terminal_audit = self.artifact_store.read_json(terminal_audit_key)
            except BaseException:
                _cleanup_initial_ownership()
                raise

            def _result_from_terminal_audit(
                audit: Mapping[str, Any],
            ) -> RebuildRunResult:
                return RebuildRunResult(
                    run_id=run_id,
                    outcome=str(audit["outcome"]),
                    reason=str(audit["reason"]),
                    audit_ref=self.artifact_store.reference(terminal_audit_key),
                    previous_kg_generation_id=audit.get("previous_kg_generation_id"),
                    current_kg_generation_id=audit.get("current_kg_generation_id"),
                    started_at=str(audit["started_at"]),
                    finished_at=str(audit["finished_at"]),
                    affected_files=tuple(
                        str(value) for value in audit.get("affected_files", ())
                    ),
                    report_ref=audit.get("report_ref"),
                    report_id=audit.get("report_id"),
                    publishable_status=audit.get("publishable_status"),
                    promotion_outcome=audit.get("promotion_outcome"),
                    operator_action=audit.get("operator_action"),
                    event_emitted=bool(audit.get("event_emitted")),
                )

            def _terminal_reconciliation_result(
                evidence: Sequence[str],
            ) -> RebuildRunResult:
                current = None
                candidate_value = (
                    existing_receipt.get("candidate_kg_generation_id")
                    if existing_receipt is not None
                    else None
                )
                if "generation_promoted" in evidence:
                    current = str(candidate_value) if candidate_value else None
                return RebuildRunResult(
                    run_id=run_id,
                    outcome=RebuildOutcome.REBUILD_FAILED.value,
                    reason=RebuildBlockReason.LEASE_LOST.value,
                    audit_ref=(
                        self.artifact_store.reference(terminal_audit_key)
                        if terminal_audit is not None
                        else ""
                    ),
                    previous_kg_generation_id=(
                        existing_receipt.get("previous_kg_generation_id")
                        if existing_receipt is not None
                        else None
                    ),
                    current_kg_generation_id=current,
                    started_at=started_at.isoformat(),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    affected_files=(),
                    report_ref=(
                        terminal_audit.get("report_ref")
                        if terminal_audit is not None
                        else None
                    ),
                    report_id=(
                        terminal_audit.get("report_id")
                        if terminal_audit is not None
                        else None
                    ),
                    publishable_status=(
                        terminal_audit.get("publishable_status")
                        if terminal_audit is not None
                        else None
                    ),
                    promotion_outcome=(
                        terminal_audit.get("promotion_outcome")
                        if terminal_audit is not None
                        else None
                    ),
                    operator_action="terminal_reconciliation_required",
                    event_emitted=bool(
                        terminal_audit is not None
                        and terminal_audit.get("event_emitted")
                    ),
                )

            if terminal_audit is not None:
                terminal_binding_matches = all(
                    (
                        terminal_audit.get("run_id") == run_id,
                        terminal_audit.get("board_id") == board_id,
                        terminal_audit.get("actor_id") == actor_id,
                        terminal_audit.get("operation") == operation,
                        terminal_audit.get("manifest_ref") == manifest_ref,
                        terminal_audit.get("user_reason") == reason,
                        terminal_audit.get("confirmation_ref")
                        == existing_receipt.get("confirmation_ref"),
                    )
                )
                if not terminal_binding_matches:
                    _cleanup_initial_ownership()
                    raise RuntimeError("rebuild_terminal_audit_binding_mismatch")
            terminal_declares_success = is_rebuild_terminal_audit_frozen(terminal_audit)
            terminal_is_frozen = False
            if terminal_declares_success:
                if self.generation_repository is None:
                    _cleanup_initial_ownership()
                    raise RuntimeError("rebuild_terminal_generation_repository_missing")
                try:
                    terminal_is_frozen = self.generation_repository.get_current(
                        board_id
                    ) == terminal_audit.get("current_kg_generation_id")
                except BaseException:
                    _cleanup_initial_ownership()
                    raise
                if not terminal_is_frozen:
                    _cleanup_initial_ownership()
                    raise RuntimeError("rebuild_terminal_generation_pointer_conflict")
            terminal_is_closed = is_rebuild_terminal_audit_closed(terminal_audit)
            if (
                recovery_failure_code is not None
                and terminal_audit is not None
                and is_rebuild_terminal_audit_resumable(terminal_audit)
                and any(
                    (
                        terminal_audit.get("report_ref"),
                        terminal_audit.get("current_kg_generation_id"),
                        terminal_audit.get("event_emitted") is True,
                    )
                )
            ):
                # A report/promotion/event journal already exists. Source drift
                # cannot authorize graph compensation after an irreversible
                # terminal effect may have happened. Keep the active receipt
                # and return the durable retry decision for operator/executor
                # reconciliation without touching the checkpoint.
                try:
                    evidence = ["resumable_run_audit"]
                    if terminal_audit.get("report_ref"):
                        evidence.append("report_persisted")
                    if terminal_audit.get("current_kg_generation_id"):
                        evidence.append("generation_promoted")
                    if terminal_audit.get("event_emitted"):
                        evidence.append("event_published")
                    return _terminal_reconciliation_result(evidence)
                finally:
                    _cleanup_initial_ownership()
            if terminal_is_frozen or (
                terminal_is_closed and not terminal_declares_success
            ):
                try:
                    _archive_confirmation_receipt(existing_receipt)
                    return _result_from_terminal_audit(terminal_audit)
                finally:
                    _cleanup_initial_ownership()

            if recovery_failure_code is not None:
                partial_terminal_evidence: list[str] = []
                report_inspector = getattr(self.report_store, "inspect_for_run", None)
                try:
                    if self.report_store is not None:
                        if not callable(report_inspector):
                            partial_terminal_evidence.append("report_state_unprovable")
                        elif (
                            report_inspector(board_id=board_id, run_id=run_id)
                            is not None
                        ):
                            partial_terminal_evidence.append("report_persisted")
                    candidate_value = existing_receipt.get("candidate_kg_generation_id")
                    if (
                        candidate_value is not None
                        and self.generation_repository is not None
                    ):
                        previous_value = existing_receipt.get(
                            "previous_kg_generation_id"
                        )
                        observed_current = self.generation_repository.get_current(
                            board_id
                        )
                        if observed_current == candidate_value:
                            partial_terminal_evidence.append("generation_promoted")
                        elif observed_current != previous_value:
                            partial_terminal_evidence.append(
                                "generation_pointer_conflict"
                            )
                        history_loader = getattr(
                            self.generation_repository,
                            "load_history",
                            None,
                        )
                        if not callable(history_loader):
                            partial_terminal_evidence.append(
                                "generation_history_unprovable"
                            )
                        else:
                            history = history_loader(board_id, str(candidate_value))
                            if history is not None:
                                if any(
                                    (
                                        history.get("board_id") != board_id,
                                        history.get("kg_generation_id")
                                        != candidate_value,
                                        history.get("run_id") != run_id,
                                    )
                                ):
                                    partial_terminal_evidence.append(
                                        "generation_history_conflict"
                                    )
                                else:
                                    partial_terminal_evidence.append(
                                        "generation_history_persisted"
                                    )
                except BaseException:
                    _cleanup_initial_ownership()
                    raise
                if partial_terminal_evidence:
                    try:
                        return _terminal_reconciliation_result(
                            partial_terminal_evidence
                        )
                    finally:
                        _cleanup_initial_ownership()
        else:
            terminal_receipt_to_replace: Mapping[str, Any] | None = None
            if existing_receipt is not None:
                try:
                    terminal_reusable = _terminal_receipt_is_closed(existing_receipt)
                except BaseException:
                    _cleanup_initial_ownership()
                    raise
                if terminal_reusable:
                    try:
                        if not _initial_fences_valid():
                            raise RuntimeError(
                                "administrative_fence_lost_before_active_rotation"
                            )
                        terminal_receipt_to_replace = dict(existing_receipt)
                        existing_receipt = None
                    except BaseException:
                        _cleanup_initial_ownership()
                        raise
                else:
                    try:
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
                            detail="authorization_already_recorded",
                        )
                    finally:
                        _cleanup_initial_ownership()
            try:
                conf_result = self.confirmation_store.consume(
                    confirmation_id=confirmation_id,
                    expected_board_id=board_id,
                    expected_actor_id=actor_id,
                    expected_operation=operation,
                    expected_preflight_hash=preflight_hash,
                    expected_manifest_ref=manifest_ref,
                    consumption_receipt_key=confirmation_receipt_key,
                    consumption_receipt_payload=confirmation_receipt,
                    expected_terminal_receipt=terminal_receipt_to_replace,
                )
            except BaseException:
                _cleanup_initial_ownership()
                raise
            if conf_result.outcome != ConfirmationOutcome.CONSUMED.value:
                try:
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
                finally:
                    _cleanup_initial_ownership()

        confirmation_receipt["confirmation_ref"] = authorized_confirmation_ref
        authorized_receipt = dict(existing_receipt or confirmation_receipt)

        def _finalise_with_release(**kwargs: Any) -> RebuildRunResult:
            return self._finalise_with_release(
                confirmation_ref_override=authorized_confirmation_ref,
                archive_confirmation_receipt=lambda: _archive_confirmation_receipt(
                    authorized_receipt
                ),
                **kwargs,
            )

        rebaseline_evidence_proved = False
        rebaseline_source_rows: tuple[Mapping[str, Any], ...] | None = None
        rebaseline_evidence_id: str | None = None
        rebaseline_target_source_set_hash: str | None = None

        def _revalidate_current_manifest(*, phase: str):
            """Re-enumerate only while both administrative fences are held.

            The pre-step check closes the confirmation→writer race.  The
            post-drain check closes the writer-delegation race where a normal
            relational event can legitimately change a source while the
            candidate queue is being consumed.
            """

            nonlocal recovery_failure_code, recovery_failure_detail
            nonlocal rebaseline_evidence_proved
            nonlocal rebaseline_evidence_id, rebaseline_source_rows
            nonlocal rebaseline_target_source_set_hash
            if recovery_failure_code is not None:
                # This invocation is authorized only to compensate the
                # persisted checkpoint. Never consult mutable live sources or
                # turn stale checkpoint rows back into rebuild authority.
                return None
            if self.source_enumerator is None:
                return None
            if manifest is None:
                raise RuntimeError("rebuild_manifest_missing_during_revalidation")
            try:
                current_source_set = self.source_enumerator.enumerate(board_id=board_id)
                revalidation = self.manifest_store.classify_revalidation(
                    manifest=manifest,
                    current_source_set=current_source_set,
                )
            except Exception as exc:
                if _resume_run_id is None:
                    raise
                recovery_failure_code = "manifest_drift"
                recovery_failure_detail = (
                    f"manifest_revalidation_exception:{phase}:{type(exc).__name__}"
                )
                return None
            from okto_pulse.core.kg.rebuild_sources import SourceSetRevalidation

            if revalidation.outcome is SourceSetRevalidation.REBASELINE:
                if (
                    rebaseline_target_source_set_hash is not None
                    and revalidation.to_source_set_hash
                    != rebaseline_target_source_set_hash
                ):
                    from okto_pulse.core.kg.rebuild_sources import (
                        RevalidationResult,
                    )

                    logger.warning(
                        "kg.rebuild_service.spec_manifest_rebaseline_target_drift "
                        "board=%s run=%s phase=%s expected=%s observed=%s",
                        board_id,
                        run_id,
                        phase,
                        rebaseline_target_source_set_hash,
                        revalidation.to_source_set_hash,
                    )
                    return RevalidationResult(SourceSetRevalidation.MANIFEST_DRIFT)
                # The compatibility hash proves semantic equivalence; this is
                # a schema migration, not content drift.  Keep the phase in the
                # audit log because a post-drain rebaseline is independently
                # fenced and intentional.
                logger.info(
                    "kg.rebuild_service.spec_manifest_rebaseline board=%s "
                    "run=%s phase=%s from_manifest_version=%d "
                    "rebaselined_count=%d rebaselined_source_refs=%s",
                    board_id,
                    run_id,
                    phase,
                    manifest.manifest_schema_version,
                    len(revalidation.rebaselined_source_refs),
                    list(revalidation.rebaselined_source_refs)[:50],
                )
                if phase == "pre_step" and not rebaseline_evidence_proved:
                    evidence_id = f"{run_id}:{manifest_ref}"
                    self.manifest_store.record_rebaseline(
                        manifest=manifest,
                        result=revalidation,
                        evidence_id=evidence_id,
                        # This callback runs inside the artifact-store
                        # transaction.  It therefore linearizes the evidence
                        # append against erasure instead of trusting the
                        # pre-call heartbeat observation.
                        fence_valid=lambda: bool(
                            lease_active
                            and _renew_operation_reservation()
                            and _renew_current_lease()
                        ),
                    )
                    rows: list[Mapping[str, Any]] = []
                    for row in current_source_set.materializable_sources:
                        payload = row.to_dict()
                        payload["_rebuild_manifest_created_at"] = manifest.created_at
                        payload["_rebuild_rebaseline_evidence_id"] = evidence_id
                        rows.append(payload)
                    for row in current_source_set.skipped_expired_working:
                        if (
                            row.artifact_type != "code_evidence"
                            or row.source_artifact_status != "superseded"
                        ):
                            continue
                        payload = row.to_dict()
                        payload["_rebuild_manifest_created_at"] = manifest.created_at
                        payload["_rebuild_dependency_closure_candidate"] = (
                            "code_evidence_supersedence"
                        )
                        payload["_rebuild_rebaseline_evidence_id"] = evidence_id
                        rows.append(payload)
                    rebaseline_source_rows = tuple(rows)
                    rebaseline_evidence_id = evidence_id
                    rebaseline_target_source_set_hash = revalidation.to_source_set_hash
                    rebaseline_evidence_proved = True
            return revalidation

        # 5. Run inside admin guard + safe lifecycle.
        affected: tuple[str, ...] = ()
        previous_generation: str | None = None
        current_generation: str | None = None
        step_result_holder: dict[str, Any] = {}
        lease_heartbeat: _RebuildLeaseHeartbeat | None = None
        lease_active = True
        write_guard_stack = ExitStack()

        def _cleanup_setup_failure() -> None:
            """Best-effort reverse-order cleanup for every setup crash."""

            try:
                write_guard_stack.close()
            except BaseException:
                logger.exception(
                    "kg.rebuild.setup_guard_cleanup_failed board=%s",
                    board_id,
                )
            if lease_heartbeat is not None:
                try:
                    lease_heartbeat.stop()
                except BaseException:
                    logger.exception(
                        "kg.rebuild.setup_writer_heartbeat_stop_failed board=%s",
                        board_id,
                    )
            try:
                self.single_writer_lock.release(
                    board_id=board_id,
                    owner_token=owner_token,
                )
            except BaseException:
                logger.exception(
                    "kg.rebuild.setup_writer_release_failed board=%s",
                    board_id,
                )
            try:
                reservation_heartbeat.stop()
            except BaseException:
                logger.exception(
                    "kg.rebuild.setup_reservation_heartbeat_stop_failed board=%s",
                    board_id,
                )
            try:
                operation_reservation.release(
                    board_id=board_id,
                    owner_token=reservation_token,
                )
            except BaseException:
                logger.exception(
                    "kg.rebuild.setup_reservation_release_failed board=%s",
                    board_id,
                )

        # KG-02.4 — pre-supply candidate generation if the repository is
        # wired, so the step adapter and downstream report carry a real
        # UUID v4 (TR3). Previous generation pointer comes from the
        # repository so promotion can detect drift.
        candidate_generation_id: str | None = None
        try:
            if self.generation_repository is not None:
                from okto_pulse.core.kg.rebuild_generation import (
                    generate_kg_generation_id,
                    is_valid_kg_generation_id,
                )

                if recovery_failure_code is not None:
                    candidate_value = authorized_receipt.get(
                        "candidate_kg_generation_id"
                    )
                    previous_value = authorized_receipt.get("previous_kg_generation_id")
                    if candidate_value is not None:
                        candidate_generation_id = str(candidate_value)
                        if not is_valid_kg_generation_id(candidate_generation_id):
                            raise RuntimeError(
                                "rebuild_confirmation_receipt_candidate_invalid"
                            )
                    previous_generation = (
                        str(previous_value) if previous_value is not None else None
                    )
                    if (
                        previous_generation is not None
                        and not is_valid_kg_generation_id(previous_generation)
                    ):
                        raise RuntimeError(
                            "rebuild_confirmation_receipt_previous_invalid"
                        )
                elif "candidate_kg_generation_id" in authorized_receipt:
                    candidate_generation_id = str(
                        authorized_receipt.get("candidate_kg_generation_id") or ""
                    )
                    if not is_valid_kg_generation_id(candidate_generation_id):
                        raise RuntimeError(
                            "rebuild_confirmation_receipt_candidate_invalid"
                        )
                    previous_value = authorized_receipt.get("previous_kg_generation_id")
                    previous_generation = (
                        str(previous_value) if previous_value is not None else None
                    )
                    if (
                        previous_generation is not None
                        and not is_valid_kg_generation_id(previous_generation)
                    ):
                        raise RuntimeError(
                            "rebuild_confirmation_receipt_previous_invalid"
                        )
                else:
                    previous_generation = self.generation_repository.get_current(
                        board_id
                    )
                    candidate_generation_id = generate_kg_generation_id()
                    if not _initial_fences_valid():
                        return _finalise_with_release(
                            run_id=run_id,
                            outcome=RebuildOutcome.REBUILD_FAILED,
                            reason=RebuildBlockReason.LEASE_LOST,
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
                            detail=(
                                "administrative fences lost before candidate "
                                "receipt binding"
                            ),
                            triggered_by=actor_id,
                            step_result=None,
                            candidate_kg_generation_id=None,
                            operation_reservation=operation_reservation,
                            reservation_token=reservation_token,
                            reservation_heartbeat=reservation_heartbeat,
                            writer_fenced=False,
                        )

                    def _bind_candidate(
                        current: Mapping[str, Any] | None,
                    ) -> Mapping[str, Any]:
                        if current is None or any(
                            current.get(key) != confirmation_receipt.get(key)
                            for key in (
                                "schema_version",
                                "run_id",
                                "board_id",
                                "actor_id",
                                "operation",
                                "preflight_hash",
                                "manifest_ref",
                                "source_set_hash",
                                "confirmation_ref",
                                "user_reason",
                                "started_at",
                            )
                        ):
                            raise RuntimeError(
                                "rebuild_confirmation_receipt_binding_drift"
                            )
                        return {
                            **dict(current),
                            "previous_kg_generation_id": previous_generation,
                            "candidate_kg_generation_id": candidate_generation_id,
                        }

                    authorized_receipt = dict(
                        self.confirmation_store.artifact_store.replace_json(
                            confirmation_receipt_key,
                            _bind_candidate,
                        )
                    )
            write_guard_stack.enter_context(
                under_safe_write(board_id, owner_token, operation)
            )
        except Exception as exc:
            logger.error(
                "kg.rebuild.pre_step_setup_failed board=%s err=%s",
                board_id,
                exc,
            )
            return _finalise_with_release(
                run_id=run_id,
                outcome=RebuildOutcome.REBUILD_FAILED,
                reason=RebuildBlockReason.GENERATION_STORE_UNAVAILABLE,
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                confirmation_id=confirmation_id,
                manifest_ref=manifest_ref,
                user_reason=reason,
                started_at=started_at,
                owner_token=owner_token,
                affected_files=(),
                previous_kg_generation_id=None,
                current_kg_generation_id=None,
                detail=f"pre_step_setup_exception={type(exc).__name__}",
                triggered_by=actor_id,
                step_result=None,
                candidate_kg_generation_id=None,
                operation_reservation=operation_reservation,
                reservation_token=reservation_token,
                reservation_heartbeat=reservation_heartbeat,
                writer_fenced=lease_active,
            )
        except BaseException:
            _cleanup_setup_failure()
            raise

        def _heartbeat_for(token: str) -> _RebuildLeaseHeartbeat:
            return _RebuildLeaseHeartbeat(
                lambda token=token: self.single_writer_lock.renew(
                    board_id=board_id,
                    owner_token=token,
                    ttl_seconds=self.lock_ttl_seconds,
                ),
                board_id=board_id,
                interval_seconds=heartbeat_interval_seconds,
            )

        def _renew_current_lease() -> bool:
            current = lease_heartbeat
            return bool(lease_active and current is not None and current.renew_now())

        def _renew_operation_reservation() -> bool:
            return bool(
                _recovery_execution_valid() and reservation_heartbeat.renew_now()
            )

        def _release_writer_for_drain() -> bool:
            nonlocal lease_active, lease_heartbeat
            if not lease_active or lease_heartbeat is None:
                return False
            if not _renew_operation_reservation():
                return False
            # Stop and detach the heartbeat before releasing. A late renewal
            # must never extend a token after the queue worker is authorized.
            try:
                lease_heartbeat.stop()
            except BaseException:
                logger.exception(
                    "kg.rebuild.drain_handoff_heartbeat_stop_failed board=%s",
                    board_id,
                )
                return False
            lease_heartbeat = None
            # Remove token A from the same-thread barrier before relinquishing
            # its physical lock. The drain window intentionally has no admin
            # write authority; the ordinary worker establishes its own guard.
            try:
                write_guard_stack.close()
            except BaseException:
                logger.exception(
                    "kg.rebuild.drain_handoff_guard_close_failed board=%s",
                    board_id,
                )
                return False
            try:
                released = bool(
                    self.single_writer_lock.release(
                        board_id=board_id,
                        owner_token=owner_token,
                    )
                )
            except Exception:
                logger.exception(
                    "kg.rebuild.drain_handoff_release_failed board=%s",
                    board_id,
                )
                lease_active = False
                return False
            lease_active = False
            return released

        def _reacquire_writer_after_drain() -> str | None:
            nonlocal lease_active, lease_heartbeat, owner_token
            if lease_active:
                return owner_token
            if not _renew_operation_reservation():
                return None
            deadline = time.monotonic() + self.lease_reacquire_timeout_seconds
            while True:
                if not _renew_operation_reservation():
                    return None
                acquisition = self.single_writer_lock.acquire(
                    board_id=board_id,
                    operation=f"kg02_rebuild:{operation}:post_drain",
                    owner_id=actor_id,
                    ttl_seconds=self.lock_ttl_seconds,
                    admin_lane=True,
                )
                if acquisition.acquired and acquisition.owner_token:
                    candidate_token = acquisition.owner_token
                    # From this instant candidate B, not released A, is the
                    # only physical token cleanup may need to retry. Rebind the
                    # local owner before any later reservation probe can fail.
                    owner_token = candidate_token
                    lease_active = True
                    if not _renew_operation_reservation():
                        try:
                            released = bool(
                                self.single_writer_lock.release(
                                    board_id=board_id,
                                    owner_token=candidate_token,
                                )
                            )
                            lease_active = not released
                        except BaseException:
                            # Keep B as the authoritative cleanup token. The
                            # service finalizer retries release in all paths.
                            lease_active = True
                            logger.exception(
                                "kg.rebuild.reacquire_reservation_loss_"
                                "release_failed board=%s",
                                board_id,
                            )
                        return None
                    try:
                        lease_heartbeat = _heartbeat_for(owner_token)
                        lease_heartbeat.start()
                        # Rebind the same-thread barrier to token B before any
                        # restore, lifecycle, compensation or promotion effect.
                        write_guard_stack.enter_context(
                            under_safe_write(board_id, owner_token, operation)
                        )
                    except BaseException:
                        if lease_heartbeat is not None:
                            try:
                                lease_heartbeat.stop()
                            except BaseException:
                                logger.exception(
                                    "kg.rebuild.reacquire_heartbeat_stop_failed "
                                    "board=%s",
                                    board_id,
                                )
                        lease_heartbeat = None
                        lease_active = False
                        try:
                            self.single_writer_lock.release(
                                board_id=board_id,
                                owner_token=owner_token,
                            )
                        except BaseException:
                            logger.exception(
                                "kg.rebuild.reacquire_writer_release_failed board=%s",
                                board_id,
                            )
                        raise
                    lease_active = True
                    return owner_token
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                time.sleep(min(self.lease_reacquire_poll_interval_seconds, remaining))

        try:
            lease_heartbeat = _heartbeat_for(owner_token)
            lease_heartbeat.start()
        except BaseException:
            _cleanup_setup_failure()
            raise

        try:
            with write_guard_stack:
                # Consume no mutable source before the administrative
                # reservation and exact graph-writer token are both proven.
                # This closes the old confirmation/revalidation→writer race.
                pre_step_fenced = bool(
                    lease_active
                    and _renew_operation_reservation()
                    and _renew_current_lease()
                )
                if not pre_step_fenced:
                    return _finalise_with_release(
                        run_id=run_id,
                        outcome=RebuildOutcome.REBUILD_FAILED,
                        reason=RebuildBlockReason.LEASE_LOST,
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
                        detail="administrative fence missing before manifest revalidation",
                        triggered_by=actor_id,
                        step_result=None,
                        candidate_kg_generation_id=candidate_generation_id,
                        lease_heartbeat=lease_heartbeat,
                        operation_reservation=operation_reservation,
                        reservation_token=reservation_token,
                        reservation_heartbeat=reservation_heartbeat,
                        writer_fenced=False,
                    )
                try:
                    pre_step_revalidation = _revalidate_current_manifest(
                        phase="pre_step"
                    )
                except Exception as exc:
                    from okto_pulse.core.kg.rebuild_sources import (
                        RebaselineEvidenceFenceLostError,
                    )

                    if isinstance(exc, RebaselineEvidenceFenceLostError):
                        return _finalise_with_release(
                            run_id=run_id,
                            outcome=RebuildOutcome.REBUILD_FAILED,
                            reason=RebuildBlockReason.LEASE_LOST,
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
                            detail="rebaseline evidence fence lost",
                            triggered_by=actor_id,
                            step_result=None,
                            candidate_kg_generation_id=candidate_generation_id,
                            lease_heartbeat=lease_heartbeat,
                            operation_reservation=operation_reservation,
                            reservation_token=reservation_token,
                            reservation_heartbeat=reservation_heartbeat,
                            writer_fenced=False,
                        )
                    logger.exception(
                        "kg.rebuild.manifest_revalidation_failed board=%s "
                        "run=%s phase=pre_step",
                        board_id,
                        run_id,
                    )
                    return _finalise_with_release(
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
                        owner_token=owner_token,
                        affected_files=(),
                        previous_kg_generation_id=previous_generation,
                        current_kg_generation_id=None,
                        detail=(
                            "manifest_revalidation_exception:pre_step:"
                            f"{type(exc).__name__}"
                        ),
                        triggered_by=actor_id,
                        step_result=None,
                        candidate_kg_generation_id=candidate_generation_id,
                        lease_heartbeat=lease_heartbeat,
                        operation_reservation=operation_reservation,
                        reservation_token=reservation_token,
                        reservation_heartbeat=reservation_heartbeat,
                        writer_fenced=lease_active,
                    )
                if pre_step_revalidation is not None and pre_step_revalidation.is_drift:
                    if _resume_run_id is not None:
                        recovery_failure_code = "manifest_drift"
                        recovery_failure_detail = (
                            "source_set_hash drift before rebuild step"
                        )
                    else:
                        return _finalise_with_release(
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
                            owner_token=owner_token,
                            affected_files=(),
                            previous_kg_generation_id=previous_generation,
                            current_kg_generation_id=None,
                            detail="source_set_hash drift before rebuild step",
                            triggered_by=actor_id,
                            step_result=None,
                            candidate_kg_generation_id=candidate_generation_id,
                            lease_heartbeat=lease_heartbeat,
                            operation_reservation=operation_reservation,
                            reservation_token=reservation_token,
                            reservation_heartbeat=reservation_heartbeat,
                            writer_fenced=lease_active,
                        )

                # Optional pre-step quarantine (reset/rebuild may need
                # to move existing graph files first). The quarantine
                # service is read off self and only invoked when the
                # caller supplied a non-None value.
                # KG-02.3 supplies the boundary — actual storage
                # mutation happens inside the step adapter.

                # 5a. Run the rebuild step.
                def _inner_source_revalidate() -> bool:
                    if recovery_failure_code is not None:
                        return True
                    revalidation = _revalidate_current_manifest(
                        phase="inner_post_drain"
                    )
                    return revalidation is None or not revalidation.is_drift

                step_input = RebuildStepInput(
                    board_id=board_id,
                    manifest_ref=manifest_ref,
                    source_set_hash=authorized_source_set_hash,
                    actor_id=actor_id,
                    operation=operation,
                    owner_token=owner_token,
                    previous_kg_generation_id=previous_generation,
                    candidate_kg_generation_id=candidate_generation_id,
                    cancel_requested=cancel_requested,
                    lease_renew=_renew_current_lease,
                    orchestration_renew=_renew_operation_reservation,
                    release_writer_for_drain=_release_writer_for_drain,
                    reacquire_writer_after_drain=_reacquire_writer_after_drain,
                    source_revalidate=_inner_source_revalidate,
                    recovery_failure_code=recovery_failure_code,
                    recovery_failure_detail=recovery_failure_detail,
                    rebaseline_source_rows=rebaseline_source_rows,
                    rebaseline_evidence_id=rebaseline_evidence_id,
                    rebaseline_target_source_set_hash=(
                        rebaseline_target_source_set_hash
                    ),
                )
                try:
                    step_result = self.rebuild_step_adapter(step_input)
                    if rebaseline_evidence_id is not None:
                        if step_result.rebaseline_evidence_id not in (
                            None,
                            rebaseline_evidence_id,
                        ) or step_result.rebaseline_target_source_set_hash not in (
                            None,
                            rebaseline_target_source_set_hash,
                        ):
                            raise RuntimeError(
                                "rebuild_step_rebaseline_binding_mismatch"
                            )
                        step_result = replace(
                            step_result,
                            rebaseline_evidence_id=rebaseline_evidence_id,
                            rebaseline_target_source_set_hash=(
                                rebaseline_target_source_set_hash
                            ),
                        )
                    elif (
                        step_result.rebaseline_evidence_id is not None
                        or step_result.rebaseline_target_source_set_hash is not None
                    ):
                        raise RuntimeError("rebuild_step_unproved_rebaseline_binding")
                except Exception as exc:
                    logger.exception(
                        "kg.rebuild.step_adapter_failed board=%s run=%s",
                        board_id,
                        run_id,
                    )
                    return _finalise_with_release(
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
                        lease_heartbeat=lease_heartbeat,
                        operation_reservation=operation_reservation,
                        reservation_token=reservation_token,
                        reservation_heartbeat=reservation_heartbeat,
                        writer_fenced=lease_active,
                    )

                # The adapter may deliberately hand token A to the queue and
                # must return only after token B is reacquired/rebound.  Prove
                # both authorities before inspecting its result or invoking
                # the durability lifecycle; ``heartbeat is None`` is never an
                # ownership signal.
                post_step_fenced = bool(
                    lease_active
                    and _renew_operation_reservation()
                    and _renew_current_lease()
                )
                if not post_step_fenced:
                    return _finalise_with_release(
                        run_id=run_id,
                        outcome=RebuildOutcome.REBUILD_FAILED,
                        reason=RebuildBlockReason.LEASE_LOST,
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
                        detail="administrative fence missing after rebuild step",
                        triggered_by=actor_id,
                        step_result=None,
                        candidate_kg_generation_id=candidate_generation_id,
                        lease_heartbeat=lease_heartbeat,
                        operation_reservation=operation_reservation,
                        reservation_token=reservation_token,
                        reservation_heartbeat=reservation_heartbeat,
                        writer_fenced=False,
                    )

                affected = step_result.affected_files
                step_result_holder["result"] = step_result
                if step_result.previous_kg_generation_id is not None:
                    previous_generation = step_result.previous_kg_generation_id
                current_generation = (
                    step_result.current_kg_generation_id or candidate_generation_id
                )

                if not step_result.ok:
                    lease_lost = (
                        step_result.detail is not None
                        and step_result.detail.startswith("lease_lost:")
                    )
                    manifest_drift = (
                        step_result.detail is not None
                        and step_result.detail.startswith("manifest_drift:")
                    )
                    return _finalise_with_release(
                        run_id=run_id,
                        outcome=(
                            RebuildOutcome.REBUILD_FAILED
                            if lease_lost
                            else (
                                RebuildOutcome.MANIFEST_DRIFT
                                if manifest_drift
                                else RebuildOutcome.FAILED
                            )
                        ),
                        reason=(
                            RebuildBlockReason.LEASE_LOST
                            if lease_lost
                            else (
                                RebuildBlockReason.MANIFEST_DRIFT
                                if manifest_drift
                                else RebuildBlockReason.LIFECYCLE_FAILED
                            )
                        ),
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
                        lease_heartbeat=lease_heartbeat,
                        operation_reservation=operation_reservation,
                        reservation_token=reservation_token,
                        reservation_heartbeat=reservation_heartbeat,
                        writer_fenced=lease_active,
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
                    return _finalise_with_release(
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
                        lease_heartbeat=lease_heartbeat,
                        operation_reservation=operation_reservation,
                        reservation_token=reservation_token,
                        reservation_heartbeat=reservation_heartbeat,
                        writer_fenced=lease_active,
                    )
                if not (_renew_operation_reservation() and _renew_current_lease()):
                    return _finalise_with_release(
                        run_id=run_id,
                        outcome=RebuildOutcome.REBUILD_FAILED,
                        reason=RebuildBlockReason.LEASE_LOST,
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
                        detail="single-writer lease lost after lifecycle",
                        triggered_by=actor_id,
                        step_result=step_result,
                        candidate_kg_generation_id=candidate_generation_id,
                        lease_heartbeat=lease_heartbeat,
                        operation_reservation=operation_reservation,
                        reservation_token=reservation_token,
                        reservation_heartbeat=reservation_heartbeat,
                        writer_fenced=False,
                    )
        except Exception as exc:
            # Catch-all so we never leak the lock.
            return _finalise_with_release(
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
                lease_heartbeat=lease_heartbeat,
                operation_reservation=operation_reservation,
                reservation_token=reservation_token,
                reservation_heartbeat=reservation_heartbeat,
                writer_fenced=lease_active,
            )
        except BaseException:
            # Process-terminating/cancellation exceptions are not converted to
            # domain outcomes, but the in-process lease and reservation still
            # must not survive them.  The processor reacquires token B before a
            # released-drain exception crosses this boundary.
            _cleanup_setup_failure()
            raise

        # 5c. G1 (SPEC4 card 619e58e1): per-layer materialization guard. The graph
        # is materialized + safe-written; before promoting the generation, verify
        # every partition the resolved source set expected actually materialized.
        # Fail closed (FAILED, no promotion) on a missing layer — never promote a
        # rebuild that silently lost a partition (empty_after_materialized).
        _final_step = step_result_holder.get("result")
        _layer_check = (
            _verify_materialized_layers(board_id, _final_step)
            if _final_step is not None
            else None
        )
        if _layer_check is not None:
            _layer_detail, _materialized = _layer_check
            return _finalise_with_release(
                run_id=run_id,
                outcome=RebuildOutcome.FAILED,
                reason=RebuildBlockReason.MATERIALIZED_LAYER_MISMATCH,
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
                current_kg_generation_id=None,  # do NOT promote a partition loss
                detail=_layer_detail,
                triggered_by=actor_id,
                step_result=_final_step,
                candidate_kg_generation_id=candidate_generation_id,
                lease_heartbeat=lease_heartbeat,
                operation_reservation=operation_reservation,
                reservation_token=reservation_token,
                reservation_heartbeat=reservation_heartbeat,
                writer_fenced=lease_active,
            )

        # 6. Happy path: COMPLETED.
        return _finalise_with_release(
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
            lease_heartbeat=lease_heartbeat,
            operation_reservation=operation_reservation,
            reservation_token=reservation_token,
            reservation_heartbeat=reservation_heartbeat,
            writer_fenced=lease_active,
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
        lease_heartbeat: _RebuildLeaseHeartbeat | None = None,
        operation_reservation: Any | None = None,
        reservation_token: str | None = None,
        reservation_heartbeat: _RebuildLeaseHeartbeat | None = None,
        writer_fenced: bool = True,
        confirmation_ref_override: str | None = None,
        archive_confirmation_receipt: Callable[[], None] | None = None,
    ) -> RebuildRunResult:
        reservation_fenced = bool(operation_reservation and reservation_token)

        def _renew_reservation_fence() -> bool:
            nonlocal reservation_fenced
            if not reservation_fenced:
                return False
            try:
                if reservation_heartbeat is not None:
                    reservation_fenced = reservation_heartbeat.renew_now()
                elif operation_reservation is not None and reservation_token:
                    reservation_fenced = bool(
                        operation_reservation.renew(
                            board_id=board_id,
                            owner_token=reservation_token,
                            ttl_seconds=self.lock_ttl_seconds,
                        )
                    )
                else:
                    reservation_fenced = False
            except BaseException:
                logger.exception(
                    "kg.rebuild.terminal_reservation_probe_failed board=%s run=%s",
                    board_id,
                    run_id,
                )
                reservation_fenced = False
            return reservation_fenced

        def _renew_terminal_fences() -> bool:
            if not writer_fenced:
                return False
            try:
                reservation_owned = _renew_reservation_fence()
                if not reservation_owned:
                    return False
                if lease_heartbeat is not None:
                    writer_owned = lease_heartbeat.renew_now()
                else:
                    # ``None`` is expected during the writer-free drain and
                    # can also follow a setup failure.  Prove the exact token
                    # directly; absence of a heartbeat is never authority.
                    writer_owned = self.single_writer_lock.renew(
                        board_id=board_id,
                        owner_token=owner_token,
                        ttl_seconds=self.lock_ttl_seconds,
                    )
                return bool(reservation_owned and writer_owned)
            except BaseException:
                logger.exception(
                    "kg.rebuild.terminal_fence_probe_failed board=%s run=%s",
                    board_id,
                    run_id,
                )
                return False

        try:
            if writer_fenced and not _renew_terminal_fences():
                outcome = RebuildOutcome.REBUILD_FAILED
                reason = RebuildBlockReason.LEASE_LOST
                current_kg_generation_id = None
                detail = (
                    f"{detail}; " if detail else ""
                ) + "administrative fence lost before terminal report"
                writer_fenced = False
            elif not writer_fenced:
                _renew_reservation_fence()
            # KG-02.4 report-first terminal gate. Reports, generation
            # promotion, terminal events and audit/counter effects remain under
            # the same board writer fence as the graph rebuild. Releasing here
            # earlier allows a concurrent board erasure to verify absence and
            # then have this finalisation recreate board-scoped state.
            if writer_fenced:
                try:
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
                        fence_renew=_renew_terminal_fences,
                    )
                except _TerminalFenceLost as exc:
                    outcome = RebuildOutcome.REBUILD_FAILED
                    reason = RebuildBlockReason.LEASE_LOST
                    report_first_decision = {
                        "outcome": outcome,
                        "reason": reason,
                        "current_kg_generation_id": None,
                        "detail": str(exc),
                        "report_ref": None,
                        "report_id": None,
                        "publishable_status": None,
                        "promotion_outcome": None,
                        "operator_action": "reacquire_writer_and_resume",
                        "event_emitted": False,
                    }
                    report_first_decision.update(exc.durable_decision)
                    # Durable refs/current pointers survive a later fence loss;
                    # only the unfinished terminal phase is blocked for resume.
                    report_first_decision["outcome"] = outcome
                    report_first_decision["reason"] = reason
                    report_first_decision["detail"] = str(exc)
                    writer_fenced = False
            else:
                # Reacquisition contention leaves only the orchestration
                # reservation live.  Do not scan graph state, run lifecycle or
                # touch generation/report promotion without token B; persist
                # only the technical audit/counter outcome.
                report_first_decision = {
                    "outcome": outcome,
                    "reason": reason,
                    "current_kg_generation_id": None,
                    "detail": detail,
                    "report_ref": None,
                    "report_id": None,
                    "publishable_status": None,
                    "promotion_outcome": None,
                    "operator_action": "reacquire_writer_and_resume",
                    "event_emitted": False,
                }

            if writer_fenced and not _renew_terminal_fences():
                writer_fenced = False
                report_first_decision["outcome"] = RebuildOutcome.REBUILD_FAILED
                report_first_decision["reason"] = RebuildBlockReason.LEASE_LOST
                report_first_decision["operator_action"] = (
                    report_first_decision["operator_action"]
                    or "resume_terminal_finalisation"
                )
                report_first_decision["detail"] = (
                    f"{report_first_decision['detail']}; "
                    if report_first_decision["detail"]
                    else ""
                ) + "administrative fence lost before technical audit"

            # A failed combined R+B probe may mean either writer loss with R
            # still owned or loss of the administrative reservation itself.
            # Never reuse the previous boolean: prove R in isolation at the
            # exact board-audit cut so erasure ownership cannot be mistaken for
            # permission to recreate a run_audit artifact.
            reservation_fenced = _renew_reservation_fence()

            final_result = self._emit_audit_and_counter(
                run_id=run_id,
                outcome=report_first_decision["outcome"],
                reason=report_first_decision["reason"],
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                confirmation_id=confirmation_id,
                confirmation_ref_override=confirmation_ref_override,
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
                persist_board_audit=reservation_fenced,
            )
            # The technical audit and active-receipt archive are two distinct
            # board-scoped writes.  Re-prove the exact authority after the
            # audit before the archive is allowed to run; otherwise erasure
            # may acquire the reservation, purge the board, and have this
            # finalizer recreate the active receipt behind it.  The lane that
            # never reacquired writer B is permitted to write only while the
            # administrative reservation itself is still live.
            post_audit_fenced = (
                _renew_terminal_fences()
                if writer_fenced
                else _renew_reservation_fence()
            )
            if not post_audit_fenced:
                # Do not archive and do not perform a compensating board write
                # without authority.  If erasure acquired R and purged the
                # audit after our fenced write, deletion here would itself race
                # the new owner; if it has not, the audit remains evidence of a
                # write that completed while R was still proven.
                durable_terminal_effect = bool(
                    final_result.report_ref
                    or final_result.current_kg_generation_id
                    or final_result.event_emitted
                )
                return RebuildRunResult(
                    run_id=final_result.run_id,
                    outcome=RebuildOutcome.REBUILD_FAILED.value,
                    reason=RebuildBlockReason.LEASE_LOST.value,
                    audit_ref="",
                    previous_kg_generation_id=(final_result.previous_kg_generation_id),
                    current_kg_generation_id=final_result.current_kg_generation_id,
                    started_at=final_result.started_at,
                    finished_at=final_result.finished_at,
                    affected_files=final_result.affected_files,
                    report_ref=final_result.report_ref,
                    report_id=final_result.report_id,
                    publishable_status=final_result.publishable_status,
                    promotion_outcome=final_result.promotion_outcome,
                    operator_action=(
                        "terminal_reconciliation_required"
                        if durable_terminal_effect
                        else (
                            final_result.operator_action
                            or "reacquire_writer_and_resume"
                        )
                    ),
                    event_emitted=final_result.event_emitted,
                )
            if (
                archive_confirmation_receipt is not None
                and is_rebuild_terminal_audit_closed(
                    {
                        "outcome": final_result.outcome,
                        "reason": final_result.reason,
                        "report_ref": final_result.report_ref,
                        "report_id": final_result.report_id,
                        "publishable_status": final_result.publishable_status,
                        "promotion_outcome": final_result.promotion_outcome,
                        "current_kg_generation_id": final_result.current_kg_generation_id,
                        "event_emitted": final_result.event_emitted,
                        "operator_action": final_result.operator_action,
                    }
                )
            ):
                archive_confirmation_receipt()
            return final_result
        finally:
            # Always release the lock — even when report/audit finalisation
            # fails. Stop the heartbeat immediately before release so it cannot
            # renew a token that has already been relinquished.
            if lease_heartbeat is not None:
                try:
                    lease_heartbeat.stop()
                except BaseException:
                    logger.exception(
                        "kg.rebuild.writer_heartbeat_stop_failed board=%s",
                        board_id,
                    )
            try:
                self.single_writer_lock.release(
                    board_id=board_id,
                    owner_token=owner_token,
                )
            except BaseException as exc:
                logger.error(
                    "kg.rebuild.lock_release_failed board=%s err=%s",
                    board_id,
                    exc,
                )
            if reservation_heartbeat is not None:
                try:
                    reservation_heartbeat.stop()
                except BaseException:
                    logger.exception(
                        "kg.rebuild.reservation_heartbeat_stop_failed board=%s",
                        board_id,
                    )
            if operation_reservation is not None and reservation_token is not None:
                try:
                    operation_reservation.release(
                        board_id=board_id,
                        owner_token=reservation_token,
                    )
                except BaseException as exc:
                    logger.error(
                        "kg.rebuild.reservation_release_failed board=%s err=%s",
                        board_id,
                        exc,
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
        fence_renew: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """If KG-02.4 primitives are wired, build + persist a report,
        run the terminal-state guard, optionally promote the generation,
        and emit ``kg.rebuilt``. Returns a dict that updates the final
        ``RebuildRunResult``.

        The legacy path is available only when the complete KG-02.4 bundle is
        absent. A partially wired generation/report/guard bundle fails closed;
        it can neither advertise a candidate as current nor skip promotion.
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

        durable_decision = {
            "report_ref": None,
            "report_id": None,
            "publishable_status": None,
            "promotion_outcome": None,
            "current_kg_generation_id": None,
            "operator_action": "resume_terminal_finalisation",
            "event_emitted": False,
        }

        def _require_terminal_fence(phase: str) -> None:
            if fence_renew is not None and not fence_renew():
                raise _TerminalFenceLost(
                    f"administrative fence lost before terminal phase:{phase}",
                    durable_decision=durable_decision,
                )

        kg024_bundle = (
            self.generation_repository,
            self.promotion_guard,
            self.report_store,
            self.terminal_state_guard,
        )
        if all(component is None for component in kg024_bundle):
            return default
        if any(component is None for component in kg024_bundle):
            return {
                **default,
                "outcome": RebuildOutcome.REBUILD_FAILED,
                "reason": RebuildBlockReason.GENERATION_STORE_UNAVAILABLE,
                "current_kg_generation_id": None,
                "operator_action": "configure_complete_kg02_4_bundle",
                "detail": "kg02_4_terminal_bundle_partially_configured",
            }

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
            _require_terminal_fence("orphan_scan")
            zero_orphan_validation = self._build_zero_orphan_validation(
                board_id=board_id,
                generation_id=candidate_kg_generation_id,
            )
            _require_terminal_fence("orphan_scan_complete")
            if zero_orphan_validation.get("integrity_warning") or (
                zero_orphan_validation.get("zero_orphan_validation") == "unavailable"
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

        _require_terminal_fence("report_persist")
        persist_result = self.report_store.persist(payload=payload)
        durable_decision.update(
            {
                "report_ref": persist_result.report_ref,
                "report_id": persist_result.report_id,
                "detail": persist_result.detail or detail,
            }
        )
        _require_terminal_fence("terminal_state_guard")
        terminal_decision = self.terminal_state_guard.require_report_ref(
            board_id=board_id,
            run_id=run_id,
            candidate_terminal_status=candidate_terminal,
            report_persist_result=persist_result,
            previous_kg_generation_id=previous_kg_generation_id,
            kg_generation_id=candidate_kg_generation_id,
        )
        durable_decision.update(
            {
                "publishable_status": terminal_decision.publishable_status,
                "operator_action": (
                    terminal_decision.operator_action or "resume_terminal_finalisation"
                ),
            }
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
            if (
                persist_result.outcome
                == ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value
            ):
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
                    _require_terminal_fence("promotion_guard")
                    structural_hash = (
                        step_result.structural_hash if step_result else None
                    ) or ""
                    source_hash = (
                        step_result.source_hash if step_result else None
                    ) or ""
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
                        final_reason = RebuildBlockReason.GENERATION_PROMOTION_BLOCKED

                if guard_eligible:
                    _require_terminal_fence("generation_promotion")
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
                        final_reason = RebuildBlockReason.GENERATION_PROMOTION_BLOCKED
                        operator_action = (
                            "retry_generation_promotion"
                            if promo.outcome == "persist_failed"
                            else "start_fresh_rebuild_after_promotion_rejection"
                        )
                    durable_decision.update(
                        {
                            "promotion_outcome": promotion_outcome_value,
                            "current_kg_generation_id": promoted_generation,
                            "operator_action": (
                                "emit_terminal_event"
                                if promoted_generation is not None
                                else operator_action or "resume_terminal_finalisation"
                            ),
                        }
                    )

            # Preserve every terminal decision already made before the event
            # fence.  In particular, a guard rejection followed by fence loss
            # must not regress to the earlier generic resume instruction.
            durable_decision.update(
                {
                    "publishable_status": terminal_decision.publishable_status,
                    "promotion_outcome": promotion_outcome_value,
                    "current_kg_generation_id": promoted_generation,
                    "operator_action": (
                        "emit_terminal_event"
                        if promoted_generation is not None
                        else operator_action or "resume_terminal_finalisation"
                    ),
                }
            )

            # TR8 — kg.rebuilt event emitted whenever the report
            # persisted, regardless of promotion outcome. Operators need
            # the event to know terminal state was reached.
            event_payload = {
                "event": "kg.rebuilt",
                "board_id": board_id,
                "previous_kg_generation_id": previous_kg_generation_id,
                # Never advertise a rejected/unpromoted candidate as current.
                "kg_generation_id": promoted_generation,
                "candidate_kg_generation_id": candidate_kg_generation_id,
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
            if step_result and step_result.rebaseline_evidence_id is not None:
                event_payload.update(
                    {
                        "rebaseline_evidence_id": (step_result.rebaseline_evidence_id),
                        "rebaseline_target_source_set_hash": (
                            step_result.rebaseline_target_source_set_hash
                        ),
                    }
                )
            _require_terminal_fence("terminal_event")
            try:
                emit_result = self.event_emitter(event_payload)
                direct_accepted = getattr(emit_result, "accepted", None)
                handler_publish = getattr(emit_result, "publish", None)
                nested_accepted = getattr(handler_publish, "accepted", None)
                # A composed handler owns more than publication (currently the
                # cognitive marker).  Its direct boolean is authoritative:
                # nested publish acceptance cannot override a failed/skipped
                # downstream effect.  Legacy direct publisher results still
                # use their own ``accepted`` flag, while bare callables must
                # return the literal True.
                accepted = (
                    direct_accepted
                    if isinstance(direct_accepted, bool)
                    else bool(emit_result is True or nested_accepted is True)
                )
                if not accepted:
                    raise RuntimeError("kg_rebuilt_event_not_accepted")
                event_emitted = True
                durable_decision["event_emitted"] = True
            except Exception as exc:
                operator_action = "emit_terminal_event"
                durable_decision["operator_action"] = operator_action
                logger.error(
                    "kg.rebuild.event_emit_failed run_id=%s err=%s",
                    run_id,
                    exc,
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
                board_id,
                exc,
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
        persist_board_audit: bool = False,
        confirmation_ref_override: str | None = None,
    ) -> RebuildRunResult:
        finished_at = datetime.now(timezone.utc)
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

            if confirmation_ref_override is not None:
                normalized_ref = str(confirmation_ref_override).casefold()
                if not _is_confirmation_ref(normalized_ref):
                    raise ValueError(
                        "confirmation_ref_override must be canonical fingerprint"
                    )
                confirmation_ref = normalized_ref
            else:
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
            # New audit rows carry an explicit authority decision.  Textual
            # operator instructions remain UX only and are not the source of
            # truth for whether this exact receipt/run may be re-entered.
            "same_run_resume_allowed": bool(
                reason is RebuildBlockReason.LEASE_LOST
                or operator_action in _RESUMABLE_TERMINAL_ACTIONS
            ),
            "resume_phase": (
                operator_action
                if operator_action in _RESUMABLE_TERMINAL_ACTIONS
                else None
            ),
            "detail": detail,
        }
        audit_ref = ""
        if persist_board_audit:
            try:
                audit_key = RebuildAuditKey(
                    namespace="run_audit",
                    board_id=board_id,
                    artifact_id=run_id,
                )
                self.artifact_store.write_json_atomic(audit_key, audit_payload)
                audit_ref = self.artifact_store.reference(audit_key)
            except Exception as exc:
                logger.error(
                    "kg.rebuild.audit_persist_failed run_id=%s err=%s",
                    run_id,
                    exc,
                )

        _bump_rebuild(board_id=board_id, status=outcome.value, reason=reason.value)
        logger.warning(
            "kg.rebuild.run_finalised run_id=%s board=%s actor=%s "
            "operation=%s outcome=%s reason=%s audit_ref=%s report_ref=%s",
            run_id,
            board_id,
            actor_id,
            operation,
            outcome.value,
            reason.value,
            audit_ref,
            report_ref,
        )

        return RebuildRunResult(
            run_id=run_id,
            outcome=outcome.value,
            reason=reason.value,
            audit_ref=audit_ref,
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
    "ClosedRebuildReconciliation",
    "KGRebuildService",
    "KGRebuiltEventEmitter",
    "OrphanScanProvider",
    "REBUILD_DIRNAME",
    "RebuildBlockReason",
    "RebuildConfirmationReceiptIntegrityError",
    "RebuildOutcome",
    "RebuildRunResult",
    "RebuildStepAdapter",
    "RebuildStepInput",
    "RebuildStepResult",
    "SUPPORTED_REBUILD_OPERATIONS",
    "get_rebuild_run_count",
    "get_rebuild_run_counter_labels",
    "classify_closed_rebuild_reconciliation",
    "is_rebuild_confirmation_receipt_valid",
    "is_rebuild_terminal_audit_closed",
    "is_rebuild_terminal_audit_frozen",
    "is_rebuild_terminal_audit_resumable",
    "get_rebuild_run_samples",
    "list_rebuild_confirmation_receipts",
    "load_verified_rebuild_confirmation_receipt",
    "rebuild_active_confirmation_receipt_key",
    "rebuild_confirmation_receipt_key",
    "rebuild_operation_run_id",
    "reset_rebuild_run_counter",
]
