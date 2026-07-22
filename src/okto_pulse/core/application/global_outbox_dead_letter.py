"""Bounded policy for terminal Global Discovery outbox operations.

The use case owns selection validation, classification, idempotency and lineage
semantics.  Persistence remains behind :class:`GlobalOutboxStore`, so Core does
not import SQLAlchemy or Community models.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from threading import RLock
from typing import Any

from okto_pulse.core.ports.delivery_ledger import is_governed_delivery_attempt
from okto_pulse.core.ports.global_outbox import (
    GLOBAL_OUTBOX_DEAD_LETTER_SENTINEL,
    GLOBAL_OUTBOX_MAX_RETRIES,
    GlobalOutboxDeadLetterCursor,
    GlobalOutboxEventRecord,
    GlobalOutboxMutationConflict,
    GlobalOutboxStore,
)
from okto_pulse.core.runtime_context import runtime_state


MAX_SELECTION = 100
METRIC_LATENCY_RETENTION = 100
_REPROCESS_MARKER = "_dlq_reprocess"
_SUPERSEDED_BY = "superseded_by_dead_letter_id"
_CLASSIFICATIONS = frozenset(
    {"global_open_failure", "board_source_failure", "unclassified_failure"}
)
_logger = logging.getLogger("okto_pulse.kg.global_outbox_dead_letter")


class _GlobalOutboxDeadLetterMetricState:
    """Runtime-context-owned, thread-safe operation metric state."""

    def __init__(self) -> None:
        self._counts: Counter[tuple[str, str, str]] = Counter()
        self._latency_ms: dict[tuple[str, str], deque[float]] = defaultdict(
            lambda: deque(maxlen=METRIC_LATENCY_RETENTION)
        )
        self._lock = RLock()

    def record(
        self,
        *,
        operation: str,
        outcome: str,
        classifications: Sequence[str],
        elapsed_ms: float,
    ) -> None:
        with self._lock:
            for classification in classifications:
                self._counts[(operation, outcome, classification)] += 1
            self._latency_ms[(operation, outcome)].append(elapsed_ms)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            latency = {
                f"{operation}:{outcome}": tuple(values)
                for (operation, outcome), values in sorted(self._latency_ms.items())
            }
            return {
                "metric_name": "kg_global_outbox_dlq_operations_total",
                "counts": {
                    f"{operation}:{outcome}:{classification}": count
                    for (operation, outcome, classification), count in sorted(
                        self._counts.items()
                    )
                },
                "latency_ms": latency,
                "retention_limit_per_operation_outcome": (METRIC_LATENCY_RETENTION),
                "retained_latency_samples": sum(
                    len(values) for values in self._latency_ms.values()
                ),
            }

    def clear(self) -> None:
        with self._lock:
            self._counts.clear()
            self._latency_ms.clear()

    def clone_for_runtime(self) -> "_GlobalOutboxDeadLetterMetricState":
        clone = type(self)()
        with self._lock:
            clone._counts = self._counts.copy()
            clone._latency_ms.update(
                {
                    key: deque(values, maxlen=METRIC_LATENCY_RETENTION)
                    for key, values in self._latency_ms.items()
                }
            )
        return clone


_metric_state = runtime_state(
    "kg.global_outbox_dead_letter.metrics",
    _GlobalOutboxDeadLetterMetricState,
)


class GlobalOutboxDeadLetterError(ValueError):
    """Typed, fail-closed operator error."""

    def __init__(
        self,
        code: str,
        *,
        mutated: bool = False,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.mutated = mutated
        self.detail = dict(detail or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "mutated": self.mutated,
            **self.detail,
        }


def classify_global_outbox_dead_letter(last_error: str | None) -> str:
    """Return the bounded public classification shared by list and health."""

    text = str(last_error or "").lower()
    if any(
        marker in text
        for marker in (
            "graph_corruption",
            "graph_unavailable",
            "graph_lock_contention",
            "memoryerror",
            "bad allocation",
            "failed to open global",
            "could not open global",
        )
    ):
        return "global_open_failure"
    if any(
        marker in text
        for marker in (
            "board graph",
            "board_read",
            "source graph",
            "source inventory",
        )
    ):
        return "board_source_failure"
    return "unclassified_failure"


def global_outbox_dead_letter_metric_snapshot() -> dict[str, object]:
    """Expose a bounded in-process snapshot for operational adapters/tests."""

    state = _metric_state.resolve()
    assert isinstance(state, _GlobalOutboxDeadLetterMetricState)
    return state.snapshot()


def reset_global_outbox_dead_letter_metrics_for_tests() -> None:
    state = _metric_state.resolve()
    assert isinstance(state, _GlobalOutboxDeadLetterMetricState)
    state.clear()


def _record_metric(
    *,
    operation: str,
    outcome: str,
    started: float,
    classifications: Sequence[str] = (),
) -> None:
    labels = tuple(sorted(set(classifications))) or ("none",)
    elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
    state = _metric_state.resolve()
    assert isinstance(state, _GlobalOutboxDeadLetterMetricState)
    state.record(
        operation=operation,
        outcome=outcome,
        classifications=labels,
        elapsed_ms=elapsed_ms,
    )
    _logger.info(
        "kg global-outbox DLQ operation=%s outcome=%s count=%d latency_ms=%.3f",
        operation,
        outcome,
        len(labels),
        elapsed_ms,
        extra={
            "event": "kg.global_outbox_dlq.operation",
            "metric_name": "kg_global_outbox_dlq_operations_total",
            "operation": operation,
            "outcome": outcome,
            "classifications": labels,
            "latency_ms": elapsed_ms,
        },
    )


def _bounded_error(value: str | None) -> str | None:
    from okto_pulse.core.services.kg_health_service import safe_health_error

    return safe_health_error(
        value,
        sensitive_reason="global_outbox_error_redacted",
        max_chars=240,
    )


def _encode_cursor(cursor: GlobalOutboxDeadLetterCursor) -> str:
    raw = json.dumps(
        {"created_at": cursor.created_at.isoformat(), "id": cursor.id},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(value: str | None) -> GlobalOutboxDeadLetterCursor | None:
    if value is None:
        return None
    try:
        normalized = str(value).strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError
        padding = "=" * (-len(normalized) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(normalized + padding).decode("utf-8")
        )
        created_at = datetime.fromisoformat(str(payload["created_at"]))
        row_id = str(payload["id"]).strip()
        if not row_id:
            raise ValueError
        return GlobalOutboxDeadLetterCursor(created_at=created_at, id=row_id)
    except Exception as exc:
        raise GlobalOutboxDeadLetterError("invalid_cursor") from exc


def _normalize_ids(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(str(value).strip() for value in values)
    if not normalized:
        raise GlobalOutboxDeadLetterError("no_dlq_selected")
    if len(normalized) > MAX_SELECTION:
        raise GlobalOutboxDeadLetterError(
            "selection_too_large", detail={"max_selection": MAX_SELECTION}
        )
    if any(not value or len(value) > 256 for value in normalized):
        raise GlobalOutboxDeadLetterError("selected_id_missing_or_wrong_class")
    if len(set(normalized)) != len(normalized):
        raise GlobalOutboxDeadLetterError("duplicate_id")
    return normalized


def _normalize_reason(reason: str) -> str:
    normalized = str(reason or "").strip()
    if not normalized or len(normalized) > 256:
        raise GlobalOutboxDeadLetterError("invalid_reason")
    return normalized


def _is_terminal(row: GlobalOutboxEventRecord) -> bool:
    return row.processed_at is None and (
        row.retry_count == GLOBAL_OUTBOX_DEAD_LETTER_SENTINEL
        or row.retry_count >= GLOBAL_OUTBOX_MAX_RETRIES
    )


def _is_idempotent_replay(row: GlobalOutboxEventRecord) -> bool:
    return isinstance(row.payload.get(_REPROCESS_MARKER), dict)


class GlobalOutboxDeadLetterOperations:
    """Application service for list, explicit reprocess and verify."""

    def __init__(
        self,
        *,
        store: GlobalOutboxStore,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def list(
        self,
        *,
        context: Any,
        limit: int = 50,
        cursor: str | None = None,
        classification: str | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            bounded_limit = int(limit)
            if not 1 <= bounded_limit <= MAX_SELECTION:
                raise GlobalOutboxDeadLetterError(
                    "invalid_limit", detail={"min": 1, "max": MAX_SELECTION}
                )
            selected_classification = (
                str(classification).strip() if classification is not None else None
            )
            if (
                selected_classification is not None
                and selected_classification not in _CLASSIFICATIONS
            ):
                raise GlobalOutboxDeadLetterError("invalid_classification")
            after = _decode_cursor(cursor)
            rows = list(
                await self._store.list_terminal_events(
                    context,
                    limit=bounded_limit + 1,
                    after=after,
                )
            )
            consumed = rows[:bounded_limit]
            has_more = len(rows) > bounded_limit
            items = []
            classifications = []
            for row in consumed:
                row_classification = classify_global_outbox_dead_letter(row.last_error)
                classifications.append(row_classification)
                if (
                    selected_classification is not None
                    and row_classification != selected_classification
                ):
                    continue
                items.append(
                    {
                        "dead_letter_id": row.id,
                        "event_id": row.event_id,
                        "board_id": row.board_id,
                        "classification": row_classification,
                        "retry_count": row.retry_count,
                        "last_error": _bounded_error(row.last_error),
                        "created_at": row.created_at.isoformat(),
                    }
                )
            next_cursor = None
            if has_more and consumed:
                next_cursor = _encode_cursor(
                    GlobalOutboxDeadLetterCursor(
                        created_at=consumed[-1].created_at,
                        id=consumed[-1].id,
                    )
                )
            result = {
                "items": items,
                "next_cursor": next_cursor,
                "count": len(items),
            }
        except GlobalOutboxDeadLetterError:
            _record_metric(operation="list", outcome="rejected", started=started)
            raise
        _record_metric(
            operation="list",
            outcome="success",
            started=started,
            classifications=classifications,
        )
        return result

    async def reprocess(
        self,
        *,
        context: Any,
        dead_letter_ids: Sequence[str],
        reason: str,
    ) -> dict[str, list[str]]:
        started = time.perf_counter()
        try:
            selected_ids = _normalize_ids(dead_letter_ids)
            normalized_reason = _normalize_reason(reason)
            rows = await self._store.get_events_by_ids(context, ids=selected_ids)
            by_id = {row.id: row for row in rows}
            governed_ids = [
                row_id
                for row_id in selected_ids
                if (row := by_id.get(row_id)) is not None
                and is_governed_delivery_attempt(
                    event_id=row.event_id,
                    payload=row.payload,
                )
            ]
            if governed_ids:
                # Governed attempts have immutable physical keys.  Only the
                # daily tick may advance the durable ledger and emit attempt
                # n+1; the legacy runbook must reject the whole mixed
                # selection before touching any row.
                raise GlobalOutboxDeadLetterError(
                    "governed_delivery_attempt_tick_owned",
                    detail={
                        "rejected_count": len(governed_ids),
                        "owner": "kg.tick.daily",
                    },
                )
            requeue: list[GlobalOutboxEventRecord] = []
            already_queued: list[str] = []
            already_applied: list[str] = []
            invalid: list[str] = []
            for row_id in selected_ids:
                row = by_id.get(row_id)
                if row is None or row.payload.get(_SUPERSEDED_BY):
                    invalid.append(row_id)
                    continue
                if _is_terminal(row):
                    requeue.append(row)
                    continue
                if _is_idempotent_replay(row):
                    target = (
                        already_applied
                        if row.processed_at is not None
                        else already_queued
                    )
                    target.append(row_id)
                    continue
                invalid.append(row_id)
            if invalid:
                accepted_count = (
                    len(requeue) + len(already_queued) + len(already_applied)
                )
                code = (
                    "mixed_selection_ineligible"
                    if accepted_count
                    else "selected_id_missing_or_wrong_class"
                )
                raise GlobalOutboxDeadLetterError(
                    code,
                    detail={"rejected_count": len(invalid)},
                )
            requested_at = self._clock().isoformat()
            for row in requeue:
                payload = copy.deepcopy(row.payload)
                previous = payload.get(_REPROCESS_MARKER)
                prior_count = (
                    int(previous.get("count", 0)) if isinstance(previous, dict) else 0
                )
                payload[_REPROCESS_MARKER] = {
                    "reason": normalized_reason,
                    "requested_at": requested_at,
                    "count": prior_count + 1,
                    "source_event_id": row.event_id,
                }
                row.payload = payload
                row.retry_count = 0
                row.last_error = None
                row.processed_at = None
            if requeue:
                try:
                    await self._store.requeue_terminal_events(context, requeue)
                except GlobalOutboxMutationConflict as exc:
                    raise GlobalOutboxDeadLetterError(
                        "selection_changed",
                        detail={"rejected_count": len(requeue)},
                    ) from exc
            result = {
                "selected_ids": list(selected_ids),
                "requeued_ids": sorted(row.id for row in requeue),
                "already_queued_ids": sorted(already_queued),
                "already_applied_ids": sorted(already_applied),
                "rejected_ids": [],
            }
        except GlobalOutboxDeadLetterError:
            _record_metric(operation="reprocess", outcome="rejected", started=started)
            raise
        _record_metric(operation="reprocess", outcome="success", started=started)
        return result

    async def verify(
        self,
        *,
        context: Any,
        dead_letter_ids: Sequence[str],
    ) -> dict[str, list[dict[str, Any]]]:
        started = time.perf_counter()
        try:
            selected_ids = _normalize_ids(dead_letter_ids)
            rows = await self._store.get_events_by_ids(context, ids=selected_ids)
            by_id = {row.id: row for row in rows}
            items = []
            for row_id in selected_ids:
                row = by_id.get(row_id)
                if row is None:
                    items.append(
                        {
                            "dead_letter_id": row_id,
                            "state": "absent",
                            "event_id": None,
                            "authoritative_id": None,
                            "supersedence_chain": [],
                            "reason_code": "dead_letter_id_absent",
                        }
                    )
                    continue
                (
                    chain,
                    authoritative,
                    lineage_issue,
                ) = await self._resolve_supersedence_chain(context=context, row=row)
                if len(chain) > 1 or lineage_issue is not None:
                    state = "superseded"
                    reason_code = (
                        lineage_issue or "superseded_by_newer_global_outbox_event"
                    )
                elif _is_terminal(row):
                    state = "still_dead_lettered"
                    reason_code = "terminal_delivery_failure"
                elif row.processed_at is not None:
                    state = "applied"
                    reason_code = "reprocessed_event_applied"
                elif _is_idempotent_replay(row) and (
                    row.retry_count > 0 or bool(row.last_error)
                ):
                    state = "processing"
                    reason_code = "reprocessed_event_retrying"
                else:
                    state = "queued"
                    reason_code = "reprocessed_event_queued"
                items.append(
                    {
                        "dead_letter_id": row.id,
                        "state": state,
                        "event_id": row.event_id,
                        "authoritative_id": (
                            authoritative.id if authoritative is not None else None
                        ),
                        "supersedence_chain": chain,
                        "reason_code": reason_code,
                    }
                )
            result = {"items": items}
        except GlobalOutboxDeadLetterError:
            _record_metric(operation="verify", outcome="rejected", started=started)
            raise
        _record_metric(operation="verify", outcome="success", started=started)
        return result

    async def _resolve_supersedence_chain(
        self,
        *,
        context: Any,
        row: GlobalOutboxEventRecord,
    ) -> tuple[
        list[str],
        GlobalOutboxEventRecord | None,
        str | None,
    ]:
        chain = [row.id]
        authoritative = row
        seen = {row.id}
        while len(chain) < MAX_SELECTION:
            next_id = authoritative.payload.get(_SUPERSEDED_BY)
            if not isinstance(next_id, str) or not next_id.strip():
                break
            normalized = next_id.strip()
            if normalized in seen:
                return chain, None, "supersedence_cycle_detected"
            candidates = await self._store.get_events_by_ids(context, ids=(normalized,))
            chain.append(normalized)
            seen.add(normalized)
            if not candidates:
                return chain, None, "supersedence_target_absent"
            authoritative = candidates[0]
        next_id = authoritative.payload.get(_SUPERSEDED_BY)
        if isinstance(next_id, str) and next_id.strip():
            return chain, None, "supersedence_chain_limit_reached"
        return chain, authoritative, None


__all__ = [
    "GlobalOutboxDeadLetterError",
    "GlobalOutboxDeadLetterOperations",
    "classify_global_outbox_dead_letter",
    "global_outbox_dead_letter_metric_snapshot",
    "reset_global_outbox_dead_letter_metrics_for_tests",
]
