"""Global discovery reindex visibility (KG-02.6).

KG-02.6 ships two primitives that make sure ``discovery.lbug`` is
never silently stale after a board rebuild (BR br_1dd21e39 + IR
ir_f98042ee):

* ``GlobalDiscoveryReindexer.reindex_or_mark_pending`` (api_396302bd)
  — invokes the actual reindex adapter when available; if the adapter
  is missing or signals unavailability, marks ``reindex_pending`` with
  an explicit reason + ``job_ref`` so operators see the gap in health
  and the rebuild report.

* ``GlobalDiscoveryReindexStatusStore.record`` (api_2325f7e1) — persists
  the visible status keyed by ``(board_id, kg_generation_id)`` so the
  KG Health view and ``RebuildReportStore`` drilldown read the same
  state. Layout:

      <base>/rebuild/discovery_reindex/<board_id>/<kg_generation_id>.json

Counter ``kg_global_discovery_reindex_total`` (OR ``or_34a86124`` /
``or_80f18a3a``) bumps for every recorded outcome with bounded labels
``(board_id, reason, status)``.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    resolve_rebuild_audit_artifact_store,
)

logger = logging.getLogger("okto_pulse.kg.global_discovery_reindex")


REBUILD_DIRNAME = "rebuild"
REINDEX_DIRNAME = "discovery_reindex"


class ReindexReason(str, Enum):
    """Bounded reason vocabulary for IR ir_f98042ee trigger_reasons."""

    DISCOVERY_LBUG_AFFECTED = "discovery_lbug_affected"
    STRUCTURAL_REFERENCE_CHANGED = "structural_reference_changed"
    OPERATOR_REQUESTED = "operator_requested"


class ReindexStatus(str, Enum):
    """Bounded status vocabulary for OR or_80f18a3a + api_2325f7e1."""

    REINDEXED = "reindexed"
    REINDEX_PENDING = "reindex_pending"
    FAILED = "failed"


class ReindexErrorCode(str, Enum):
    """Bounded error codes for api_396302bd response_errors."""

    GLOBAL_DISCOVERY_UNAVAILABLE = "global_discovery_unavailable"
    INVALID_GENERATION = "invalid_generation"
    REINDEX_STATUS_STORE_UNAVAILABLE = "reindex_status_store_unavailable"


# Visible-from-Health/Report mapping. ``failed`` is recorded but the
# health surface still gates the operator: it's not the same as
# "silently stale", because the reason + job_ref are persisted.
VISIBLE_STATUSES: frozenset[str] = frozenset(
    {
        ReindexStatus.REINDEXED.value,
        ReindexStatus.REINDEX_PENDING.value,
        ReindexStatus.FAILED.value,
    }
)


@dataclass(frozen=True, slots=True)
class ReindexOutcome:
    """Frozen response for ``GlobalDiscoveryReindexer.reindex_or_mark_pending``
    matching the api_396302bd schema."""

    status: str  # ReindexStatus value
    reason: str
    report_ref: str
    board_id: str
    kg_generation_id: str
    job_ref: str | None = None
    indexed_generation: str | None = None
    error_code: str | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ReindexRecordResult:
    """Frozen response for ``GlobalDiscoveryReindexStatusStore.record``
    matching the api_2325f7e1 schema."""

    visible_in_health: bool
    visible_in_report: bool
    recorded_at: str | None
    record_ref: str | None
    error_code: str | None = None
    detail: str | None = None


# --- Counters ----------------------------------------------------------------

_REINDEX_LABELS = ("board_id", "reason", "status")
_reindex_counter: dict[tuple[str, str, str], int] = {}
_reindex_counter_lock = threading.Lock()


def _bump_reindex(*, board_id: str, reason: str, status: str) -> None:
    key = (board_id, reason, status)
    with _reindex_counter_lock:
        _reindex_counter[key] = _reindex_counter.get(key, 0) + 1


def get_reindex_count(
    board_id: str,
    *,
    reason: str | None = None,
    status: str | None = None,
) -> int:
    with _reindex_counter_lock:
        total = 0
        for (b, rsn, st), value in _reindex_counter.items():
            if b != board_id:
                continue
            if reason is not None and rsn != reason:
                continue
            if status is not None and st != status:
                continue
            total += value
        return total


def get_reindex_counter_labels() -> tuple[str, ...]:
    return _REINDEX_LABELS


def get_reindex_samples() -> list[dict[str, Any]]:
    with _reindex_counter_lock:
        return [
            {"board_id": b, "reason": rsn, "status": st, "count": value}
            for (b, rsn, st), value in _reindex_counter.items()
        ]


def reset_reindex_counter() -> None:
    with _reindex_counter_lock:
        _reindex_counter.clear()


# --- Validation helpers ----------------------------------------------------


def _validate_kg_generation_id(value: str) -> None:
    """Reuse KG-02.4 UUID v4 validation. Raises ``ValueError`` on bad
    input so callers surface ``invalid_generation`` consistently."""

    from okto_pulse.core.kg.rebuild_generation import (
        is_valid_kg_generation_id,
    )

    if not is_valid_kg_generation_id(value):
        raise ValueError("invalid kg_generation_id (must be UUID v4)")


def _coerce_reason(reason: str) -> str:
    """Coerce ``reason`` to a bounded ``ReindexReason`` value. Unknown
    reasons fall back to ``OPERATOR_REQUESTED`` so the counter label
    stays closed and the operator sees the right trigger."""

    try:
        return ReindexReason(reason).value
    except ValueError:
        return ReindexReason.OPERATOR_REQUESTED.value


# --- Reindex adapter type --------------------------------------------------


# Reindex adapter signature: receives (board_id, kg_generation_id,
# affected_refs) and returns a tuple ``(success, indexed_generation,
# job_ref, detail)``. ``success=False`` lands on the pending path with
# the supplied job_ref so the operator can poll the scheduled job.
@dataclass(frozen=True, slots=True)
class ReindexAttempt:
    success: bool
    indexed_generation: str | None = None
    job_ref: str | None = None
    detail: str | None = None
    error_code: str | None = None


ReindexAdapter = Callable[[str, str, tuple[str, ...]], ReindexAttempt]


def _default_reindex_adapter(
    _board_id: str,
    _kg_generation_id: str,
    _affected_refs: tuple[str, ...],
) -> ReindexAttempt:
    """Default adapter — marks the reindex as pending without touching
    discovery.lbug. Production wires the real reindexer; until then the
    operator sees ``reindex_pending`` and a deterministic ``job_ref``
    pointing at the manual recovery path."""

    return ReindexAttempt(
        success=False,
        job_ref="manual_reindex_required",
        detail="default adapter does not reindex; operator action required",
        error_code=ReindexErrorCode.GLOBAL_DISCOVERY_UNAVAILABLE.value,
    )


# --- Status store ----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryReindexStatusStore:
    """Store for the per-(board, generation) reindex status.

    ``record`` is the ONLY way to publish the visible status; the
    rebuilder calls it after every reindex attempt so the KG Health view
    + the rebuild report read from the same source of truth.
    """

    base_dir: object | None = None
    artifact_store: RebuildAuditArtifactStore | None = None
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    def _record_key(self, board_id: str, kg_generation_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="global_discovery_reindex",
            board_id=board_id,
            kg_generation_id=kg_generation_id,
        )

    def record(
        self,
        *,
        board_id: str,
        kg_generation_id: str,
        reason: str,
        status: str,
        job_ref: str | None = None,
        indexed_generation: str | None = None,
        detail: str | None = None,
    ) -> ReindexRecordResult:
        """Persist the visible reindex outcome.

        ``reason`` and ``status`` are coerced to the bounded vocabulary
        so the counter labels stay closed even if a future caller drifts
        from the api_2325f7e1 enum.
        """

        try:
            _validate_kg_generation_id(kg_generation_id)
        except ValueError as exc:
            return ReindexRecordResult(
                visible_in_health=False,
                visible_in_report=False,
                recorded_at=None,
                record_ref=None,
                error_code=ReindexErrorCode.INVALID_GENERATION.value,
                detail=str(exc),
            )

        bounded_reason = _coerce_reason(reason)
        try:
            bounded_status = ReindexStatus(status).value
        except ValueError:
            bounded_status = ReindexStatus.FAILED.value

        with self._lock:
            try:
                recorded_at = datetime.now(timezone.utc).isoformat()
                payload = {
                    "board_id": board_id,
                    "kg_generation_id": kg_generation_id,
                    "reason": bounded_reason,
                    "status": bounded_status,
                    "job_ref": job_ref,
                    "indexed_generation": indexed_generation,
                    "detail": detail,
                    "recorded_at": recorded_at,
                }
                key = self._record_key(board_id, kg_generation_id)
                self.artifact_store.write_json_atomic(key, payload)
                record_ref = self.artifact_store.reference(key)
            except Exception as exc:
                logger.error(
                    "kg.global_discovery_reindex.record_failed board=%s gen=%s err=%s",
                    board_id,
                    kg_generation_id,
                    exc,
                )
                _bump_reindex(
                    board_id=board_id,
                    reason=bounded_reason,
                    status=ReindexStatus.FAILED.value,
                )
                return ReindexRecordResult(
                    visible_in_health=False,
                    visible_in_report=False,
                    recorded_at=None,
                    record_ref=None,
                    error_code=ReindexErrorCode.REINDEX_STATUS_STORE_UNAVAILABLE.value,
                    detail=f"persist_exception={type(exc).__name__}",
                )

        _bump_reindex(
            board_id=board_id,
            reason=bounded_reason,
            status=bounded_status,
        )
        visible = bounded_status in VISIBLE_STATUSES
        return ReindexRecordResult(
            visible_in_health=visible,
            visible_in_report=visible,
            recorded_at=recorded_at,
            record_ref=record_ref,
        )

    def get_status(self, board_id: str, kg_generation_id: str) -> dict[str, Any] | None:
        try:
            return self.artifact_store.read_json(
                self._record_key(board_id, kg_generation_id)
            )
        except Exception as exc:
            logger.error(
                "kg.global_discovery_reindex.read_failed board=%s gen=%s err=%s",
                board_id,
                kg_generation_id,
                exc,
            )
            return None

    def latest_for_board(self, board_id: str) -> dict[str, Any] | None:
        """Return the most recently recorded reindex status for the
        board, or ``None`` if there is no record yet. ``recorded_at``
        ISO8601 sorts lexically — same key the report drilldown shows."""

        rows = self.artifact_store.list_json(
            RebuildAuditKey(
                namespace="global_discovery_reindex",
                board_id=board_id,
            )
        )
        return max(rows, key=lambda row: str(row.get("recorded_at", "")), default=None)


# --- Reindexer -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryReindexer:
    """KG-02.6 admin-lane reindexer.

    The reindexer is the ONLY documented path that publishes a visible
    discovery reindex outcome. Production wires a real
    ``reindex_adapter`` that talks to discovery.lbug; until that's
    available the default adapter marks pending so the operator sees
    the gap.
    """

    status_store: GlobalDiscoveryReindexStatusStore
    reindex_adapter: ReindexAdapter = _default_reindex_adapter

    def reindex_or_mark_pending(
        self,
        *,
        board_id: str,
        kg_generation_id: str,
        reason: str,
        affected_refs: Iterable[str] | None = None,
    ) -> ReindexOutcome:
        """Try to reindex; on failure mark pending with explicit reason.

        Returns ``ReindexOutcome`` shaped for api_396302bd. The status
        is ALWAYS persisted via ``status_store.record`` before the
        outcome returns so the operator-visible state can never drift
        from the runtime decision.
        """

        bounded_reason = _coerce_reason(reason)

        try:
            _validate_kg_generation_id(kg_generation_id)
        except ValueError as exc:
            # val_bf8c49c2 rework (item 3): bump the FAILED counter
            # explicitly so the invalid_generation path is observable in
            # OR or_34a86124 even though the store cannot persist a
            # record for it (invalid id has no valid record path).
            _bump_reindex(
                board_id=board_id,
                reason=bounded_reason,
                status=ReindexStatus.FAILED.value,
            )
            # Attempting the store still surfaces the INVALID_GENERATION
            # error_code via its own validation path — kept for
            # symmetry but the outcome below NEVER claims visibility.
            self.status_store.record(
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                reason=bounded_reason,
                status=ReindexStatus.FAILED.value,
                detail=str(exc),
            )
            return ReindexOutcome(
                status=ReindexStatus.FAILED.value,
                reason=bounded_reason,
                report_ref="",
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                error_code=ReindexErrorCode.INVALID_GENERATION.value,
                detail=str(exc),
            )

        refs = tuple(affected_refs) if affected_refs is not None else ()

        try:
            attempt = self.reindex_adapter(board_id, kg_generation_id, refs)
        except Exception as exc:
            logger.error(
                "kg.global_discovery_reindex.adapter_failed board=%s gen=%s err=%s",
                board_id,
                kg_generation_id,
                exc,
            )
            attempt = ReindexAttempt(
                success=False,
                job_ref="adapter_exception",
                detail=f"adapter_exception={type(exc).__name__}",
                error_code=ReindexErrorCode.GLOBAL_DISCOVERY_UNAVAILABLE.value,
            )

        if attempt.success:
            status_value = ReindexStatus.REINDEXED.value
        else:
            status_value = ReindexStatus.REINDEX_PENDING.value

        record = self.status_store.record(
            board_id=board_id,
            kg_generation_id=kg_generation_id,
            reason=bounded_reason,
            status=status_value,
            job_ref=attempt.job_ref,
            indexed_generation=attempt.indexed_generation
            or (kg_generation_id if attempt.success else None),
            detail=attempt.detail,
        )

        # val_bf8c49c2 rework: if the status store could not durably
        # persist the visible outcome, the runtime MUST NOT claim
        # ``reindexed`` or ``reindex_pending``. BR br_1dd21e39 forbids
        # silent staleness — and the operator-facing surface (health +
        # report) only sees what landed in the store. Without a
        # ``record_ref`` there is nothing to show, so we downgrade the
        # outcome to FAILED with the store's error_code surfaced.
        if record.error_code is not None or not record.record_ref:
            logger.error(
                "kg.global_discovery_reindex.store_failure board=%s gen=%s "
                "intended_status=%s store_error=%s",
                board_id,
                kg_generation_id,
                status_value,
                record.error_code,
            )
            return ReindexOutcome(
                status=ReindexStatus.FAILED.value,
                reason=bounded_reason,
                report_ref="",
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                job_ref=attempt.job_ref,
                indexed_generation=None,
                error_code=(
                    record.error_code
                    or ReindexErrorCode.REINDEX_STATUS_STORE_UNAVAILABLE.value
                ),
                detail=record.detail
                or attempt.detail
                or (f"intended_status={status_value} not visible due to store failure"),
            )

        return ReindexOutcome(
            status=status_value,
            reason=bounded_reason,
            report_ref=record.record_ref,
            board_id=board_id,
            kg_generation_id=kg_generation_id,
            job_ref=attempt.job_ref,
            indexed_generation=attempt.indexed_generation
            or (kg_generation_id if attempt.success else None),
            error_code=attempt.error_code if not attempt.success else None,
            detail=attempt.detail,
        )


__all__ = [
    "GlobalDiscoveryReindexStatusStore",
    "GlobalDiscoveryReindexer",
    "REBUILD_DIRNAME",
    "REINDEX_DIRNAME",
    "ReindexAdapter",
    "ReindexAttempt",
    "ReindexErrorCode",
    "ReindexOutcome",
    "ReindexReason",
    "ReindexRecordResult",
    "ReindexStatus",
    "VISIBLE_STATUSES",
    "get_reindex_count",
    "get_reindex_counter_labels",
    "get_reindex_samples",
    "reset_reindex_counter",
]
