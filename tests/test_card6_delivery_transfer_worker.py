"""Card 6: worker hand-off from stale reconciliation to GD delivery."""

from __future__ import annotations

import copy
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Iterator

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.application.processors.consolidation import (
    ConsolidationProcessor,
)
from okto_pulse.core.kg.canonical_stale_reconciler import StaleReconcileResult
from okto_pulse.core.ports.consolidation import (
    ConsolidationQueueRecord,
    get_consolidation_persistence_port,
    register_consolidation_persistence_port,
    reset_consolidation_persistence_port_for_tests,
)
from okto_pulse.core.ports.delivery_ledger import (
    DeliveryCircuitSnapshot,
    DeliveryState,
    DeliveryTransferClaimConflict,
    DeliveryTransferReplayConflict,
    DeliveryTransferRequest,
    DeliveryTransferReceipt,
    get_delivery_ledger_port,
    register_delivery_ledger_port,
    reset_delivery_ledger_port_for_tests,
)


def _entry(*, work_kind: str = "stale_reconcile") -> ConsolidationQueueRecord:
    governed = work_kind == "stale_reconcile"
    delete_event_id = "delete-card6-g1" if governed else None
    return ConsolidationQueueRecord(
        id=f"card6-{work_kind}",
        board_id="board-card6",
        artifact_type="spec",
        artifact_id="deleted-spec-card6",
        status="pending",
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=None,
        claim_timeout_at=None,
        worker_id=None,
        claimed_by_session_id=None,
        triggered_at=datetime.now(timezone.utc),
        priority="high",
        work_kind=work_kind,
        generation=1 if governed else 0,
        payload=(
            {
                "schema_version": 1,
                "delete_event_id": delete_event_id,
                "source_refs": ["spec:deleted-spec-card6"],
            }
            if governed
            else None
        ),
        delete_event_id=delete_event_id,
        claim_token=None,
    )


class _Transaction:
    def __init__(self, store: _QueueStore) -> None:
        self.store = store
        self.snapshot = store.snapshot()
        self.finished = False


class _TransactionScope:
    def __init__(self, store: _QueueStore) -> None:
        self.transaction = _Transaction(store)

    async def __aenter__(self) -> _Transaction:
        return self.transaction

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> bool:
        if exc_type is not None and not self.transaction.finished:
            self.transaction.store.auto_rollback(self.transaction)
        return False


class _QueueStore:
    def __init__(self, entry: ConsolidationQueueRecord) -> None:
        self.entries = {entry.id: entry}
        self.delivery_rows: dict[str, DeliveryState] = {}
        self.partial_writes: set[str] = set()
        self.ack_calls: list[dict[str, Any]] = []
        self.fence_calls: list[dict[str, Any]] = []
        self.commit_count = 0
        self.fail_commit_calls: set[int] = set()
        self.rollback_count = 0
        self.auto_rollback_count = 0

    def scope(self) -> _TransactionScope:
        return _TransactionScope(self)

    def snapshot(
        self,
    ) -> tuple[
        dict[str, ConsolidationQueueRecord],
        dict[str, DeliveryState],
        set[str],
    ]:
        return (
            copy.deepcopy(self.entries),
            copy.deepcopy(self.delivery_rows),
            set(self.partial_writes),
        )

    def restore(
        self,
        snapshot: tuple[
            dict[str, ConsolidationQueueRecord],
            dict[str, DeliveryState],
            set[str],
        ],
    ) -> None:
        self.entries, self.delivery_rows, self.partial_writes = copy.deepcopy(
            snapshot
        )

    def auto_rollback(self, transaction: _Transaction) -> None:
        self.restore(transaction.snapshot)
        transaction.finished = True
        self.auto_rollback_count += 1

    async def count_pending(self, _context: _Transaction) -> int:
        return sum(entry.status == "pending" for entry in self.entries.values())

    async def list_claimed_board_ids(
        self,
        _context: _Transaction,
    ) -> frozenset[str]:
        return frozenset(
            entry.board_id
            for entry in self.entries.values()
            if entry.status == "claimed"
        )

    async def list_ready_pending(
        self,
        _context: _Transaction,
        *,
        now: datetime,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        del now
        return tuple(
            entry for entry in self.entries.values() if entry.status == "pending"
        )

    async def save_queue_entries(
        self,
        _context: _Transaction,
        entries: list[ConsolidationQueueRecord],
    ) -> None:
        for entry in entries:
            self.entries[entry.id] = entry

    async def get_queue_entry(
        self,
        _context: _Transaction,
        *,
        entry_id: str,
    ) -> ConsolidationQueueRecord | None:
        return self.entries.get(entry_id)

    async def load_artifact(
        self,
        _context: _Transaction,
        **_identity: Any,
    ) -> None:
        # Governed deletion has already removed the source artifact.
        return None

    async def queue_claim_is_current_and_unfenced(
        self,
        _context: _Transaction,
        **identity: Any,
    ) -> bool:
        self.fence_calls.append(identity)
        current = self.entries.get(str(identity["entry_id"]))
        return bool(
            current is not None
            and current.status == "claimed"
            and current.claim_token == identity["claim_token"]
            and current.board_id == identity["board_id"]
            and current.artifact_type == identity["artifact_type"]
            and current.artifact_id == identity["artifact_id"]
            and current.work_kind == identity["work_kind"]
            and current.generation == identity["generation"]
            and current.delete_event_id == identity["delete_event_id"]
        )

    async def ack_claimed_queue_entry(
        self,
        _context: _Transaction,
        **identity: Any,
    ) -> bool:
        self.ack_calls.append(identity)
        current = self.entries.get(str(identity["entry_id"]))
        matched = bool(
            current is not None
            and current.status == "claimed"
            and current.claim_token == identity["claim_token"]
            and current.generation == identity["generation"]
            and current.delete_event_id == identity["delete_event_id"]
        )
        if matched and current is not None:
            self.entries.pop(current.id)
        return matched

    async def commit(self, context: _Transaction) -> None:
        self.commit_count += 1
        if self.commit_count in self.fail_commit_calls:
            raise RuntimeError(f"injected commit failure {self.commit_count}")
        context.finished = True

    async def rollback(self, context: _Transaction) -> None:
        self.restore(context.snapshot)
        context.finished = True
        self.rollback_count += 1


class _DeliveryPort:
    def __init__(
        self,
        store: _QueueStore,
        *,
        degraded: bool = False,
        failure: Exception | None = None,
        authoritative_state: DeliveryState | None = None,
    ) -> None:
        self.store = store
        self.degraded = degraded
        self.failure = failure
        self.authoritative_state = authoritative_state
        self.read_calls: list[str] = []
        self.transfer_calls: list[DeliveryTransferRequest] = []

    async def read_circuit_snapshot(
        self,
        _context: _Transaction,
        *,
        board_id: str,
    ) -> DeliveryCircuitSnapshot:
        self.read_calls.append(board_id)
        return DeliveryCircuitSnapshot(
            degraded=self.degraded,
            reason="global_circuit_open" if self.degraded else "healthy",
        )

    async def transfer_delivery_ownership(
        self,
        _context: _Transaction,
        request: DeliveryTransferRequest,
    ) -> DeliveryTransferReceipt:
        self.transfer_calls.append(request)
        # These mutations model all three effects staged in the caller's
        # transaction before the adapter learns whether its queue CAS won.
        state = self.authoritative_state or request.target_state
        self.store.delivery_rows[request.delivery_key] = state
        self.store.entries.pop(request.entry_id, None)
        if self.failure is not None:
            raise self.failure
        return DeliveryTransferReceipt(
            delivery_key=request.delivery_key,
            state=state,
            attempt=request.attempt,
            attempt_event_key=(
                request.attempt_event_key
                if state is DeliveryState.OUTBOX_PERSISTED
                else None
            ),
            replayed=self.authoritative_state is not None,
        )


@contextmanager
def _registered_ports(
    store: _QueueStore,
    delivery: _DeliveryPort,
) -> Iterator[None]:
    try:
        previous_store = get_consolidation_persistence_port()
    except RuntimeError:
        previous_store = None
    try:
        previous_delivery = get_delivery_ledger_port()
    except RuntimeError:
        previous_delivery = None

    register_consolidation_persistence_port(store)
    reset_delivery_ledger_port_for_tests()
    register_delivery_ledger_port(delivery)
    try:
        yield
    finally:
        reset_delivery_ledger_port_for_tests()
        if previous_delivery is not None:
            register_delivery_ledger_port(previous_delivery)
        if previous_store is None:
            reset_consolidation_persistence_port_for_tests()
        else:
            register_consolidation_persistence_port(previous_store)


def _processor(
    monkeypatch: pytest.MonkeyPatch,
    store: _QueueStore,
    *,
    clock: object | None = None,
) -> ConsolidationProcessor:
    from okto_pulse.core.infra import config
    from okto_pulse.core.services import queue_health_service

    monkeypatch.setattr(
        config,
        "get_settings",
        lambda: SimpleNamespace(
            kg_queue_claim_timeout_s=30,
            kg_queue_max_attempts=3,
        ),
    )
    monkeypatch.setattr(queue_health_service, "record_claim", lambda **_kwargs: None)
    return ConsolidationProcessor(store.scope, batch_size=1, clock=clock)


def _track_mark_failed(
    monkeypatch: pytest.MonkeyPatch,
    processor: ConsolidationProcessor,
) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    async def _mark_failed(
        _context: _Transaction,
        entry: ConsolidationQueueRecord,
        *,
        error_text: str,
        max_attempts: int,
    ) -> None:
        calls.append({"error_text": error_text, "max_attempts": max_attempts})
        entry.attempts += 1
        entry.last_error = error_text
        entry.status = "pending"
        entry.next_retry_at = None
        entry.claimed_at = None
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_by_session_id = None
        entry.claim_token = None

    monkeypatch.setattr(processor, "_mark_failed", _mark_failed)
    return calls


def _complete_empty_result(entry: ConsolidationQueueRecord) -> StaleReconcileResult:
    return StaleReconcileResult(
        board_id=entry.board_id,
        correlation_id=entry.delete_event_id or "legacy",
        demoted=[],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("degraded", "expected_state"),
    [
        (False, DeliveryState.OUTBOX_PERSISTED),
        (True, DeliveryState.DELIVERY_DEBT),
    ],
    ids=("healthy-outbox", "degraded-debt"),
)
async def test_complete_empty_demotion_transfers_using_circuit_target_without_legacy_ack(
    monkeypatch: pytest.MonkeyPatch,
    degraded: bool,
    expected_state: DeliveryState,
) -> None:
    entry = _entry()
    result = _complete_empty_result(entry)
    store = _QueueStore(entry)
    delivery = _DeliveryPort(store, degraded=degraded)
    processor = _processor(monkeypatch, store)
    mark_failed_calls = _track_mark_failed(monkeypatch, processor)

    async def _process(*_args: object, **_kwargs: object) -> bool:
        return consolidation._stale_reconcile_is_complete(result)

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _process)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    assert result.demoted == []
    assert processed == 1
    assert delivery.read_calls == [entry.board_id]
    assert len(delivery.transfer_calls) == 1
    request = delivery.transfer_calls[0]
    assert request.target_state is expected_state
    assert request.delete_event_id == "delete-card6-g1"
    assert store.delivery_rows == {request.delivery_key: expected_state}
    assert entry.id not in store.entries
    assert store.ack_calls == []
    assert mark_failed_calls == []


@pytest.mark.asyncio
async def test_transfer_carries_reconcile_evidence_and_controlled_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    boundary = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
    clock = SimpleNamespace(now=lambda: boundary)
    entry = _entry()
    entry.attempts = 2
    store = _QueueStore(entry)
    delivery = _DeliveryPort(store)
    processor = _processor(monkeypatch, store, clock=clock)
    result = StaleReconcileResult(
        board_id=entry.board_id,
        correlation_id=entry.delete_event_id or "missing",
        scanned=4,
        demoted=[{"node_id": "requirement-1"}],
        routed_to_debt=[{"node_id": "learning-1"}],
    )

    async def _complete(
        _context: object,
        claimed: ConsolidationQueueRecord,
        *,
        stale_reconcile_telemetry: dict[str, object],
        **_kwargs: object,
    ) -> bool:
        stale_reconcile_telemetry.update(
            consolidation._stale_reconcile_telemetry_details(result, claimed)
        )
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _complete)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    assert processed == 1
    request = delivery.transfer_calls[0]
    assert request.occurred_at == boundary
    assert dict(request.reconcile_details) == {
        "queue_attempt": 2,
        "scanned": 4,
        "demoted_count": 1,
        "routed_to_debt_count": 1,
        "incomplete": False,
        "incomplete_cause": None,
        "failed_types": [],
        "circuit_reason": "healthy",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("degraded", "authoritative_state", "requested_state"),
    [
        (
            True,
            DeliveryState.OUTBOX_PERSISTED,
            DeliveryState.DELIVERY_DEBT,
        ),
        (
            False,
            DeliveryState.DELIVERY_DEBT,
            DeliveryState.OUTBOX_PERSISTED,
        ),
    ],
    ids=("healthy-owner-after-degrade", "debt-owner-after-recovery"),
)
async def test_replay_preserves_authoritative_owner_across_circuit_change(
    monkeypatch: pytest.MonkeyPatch,
    degraded: bool,
    authoritative_state: DeliveryState,
    requested_state: DeliveryState,
) -> None:
    entry = _entry()
    store = _QueueStore(entry)
    delivery = _DeliveryPort(
        store,
        degraded=degraded,
        authoritative_state=authoritative_state,
    )
    processor = _processor(monkeypatch, store)
    mark_failed_calls = _track_mark_failed(monkeypatch, processor)

    async def _complete(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _complete)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    request = delivery.transfer_calls[0]
    assert processed == 1
    assert request.target_state is requested_state
    assert store.delivery_rows == {request.delivery_key: authoritative_state}
    assert entry.id not in store.entries
    assert mark_failed_calls == []


@pytest.mark.asyncio
async def test_transfer_claim_conflict_rolls_back_as_neutral_without_mark_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _entry()
    store = _QueueStore(entry)
    delivery = _DeliveryPort(
        store,
        failure=DeliveryTransferClaimConflict("queue CAS matched zero rows"),
    )
    processor = _processor(monkeypatch, store)
    mark_failed_calls = _track_mark_failed(monkeypatch, processor)

    async def _complete(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _complete)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    fresh = store.entries[entry.id]
    assert processed == 0
    assert store.rollback_count == 1
    assert store.auto_rollback_count == 0
    assert store.delivery_rows == {}
    assert fresh.status == "claimed"
    assert fresh.claim_token
    assert fresh.attempts == 0
    assert mark_failed_calls == []
    assert store.ack_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_type", "message"),
    [
        (DeliveryTransferReplayConflict, "divergent immutable replay"),
        (RuntimeError, "delivery adapter unavailable"),
    ],
    ids=("hard-replay-conflict", "adapter-error"),
)
async def test_hard_transfer_failures_roll_back_then_repend_current_claim(
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
    message: str,
) -> None:
    entry = _entry()
    store = _QueueStore(entry)
    delivery = _DeliveryPort(store, failure=failure_type(message))
    processor = _processor(monkeypatch, store)
    mark_failed_calls = _track_mark_failed(monkeypatch, processor)

    async def _complete(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _complete)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    fresh = store.entries[entry.id]
    assert processed == 0
    assert store.rollback_count == 1
    assert store.delivery_rows == {}
    assert fresh.status == "pending"
    assert fresh.claim_token is None
    assert fresh.attempts == 1
    assert len(mark_failed_calls) == 1
    assert message in str(mark_failed_calls[0]["error_text"])
    assert store.ack_calls == []


@pytest.mark.asyncio
async def test_stale_reconcile_transfer_failure_at_threshold_retains_governed_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _entry()
    entry.attempts = 2
    expected_payload = copy.deepcopy(entry.payload)
    store = _QueueStore(entry)
    delivery = _DeliveryPort(
        store,
        failure=RuntimeError("delivery adapter unavailable at threshold"),
    )
    processor = _processor(monkeypatch, store)
    dead_letter_calls: list[str] = []

    async def _route_to_dead_letter(
        _context: _Transaction,
        routed: ConsolidationQueueRecord,
        **_kwargs: object,
    ) -> None:
        dead_letter_calls.append(routed.id)

    async def _complete(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _complete)
    monkeypatch.setattr(consolidation, "route_to_dead_letter", _route_to_dead_letter)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    fresh = store.entries[entry.id]
    assert processed == 0
    assert store.rollback_count == 1
    assert store.delivery_rows == {}
    assert fresh.status == "pending"
    assert fresh.attempts == 3
    assert fresh.claim_token is None
    assert fresh.work_kind == "stale_reconcile"
    assert fresh.generation == 1
    assert fresh.delete_event_id == "delete-card6-g1"
    assert fresh.payload == expected_payload
    assert fresh.next_retry_at is not None
    assert fresh.next_retry_at <= datetime.now(timezone.utc) + timedelta(seconds=300)
    assert dead_letter_calls == []
    assert store.ack_calls == []


@pytest.mark.asyncio
async def test_transfer_telemetry_is_not_emitted_before_commit(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    entry = _entry()
    store = _QueueStore(entry)
    # Claim commit is first; fail the transaction that contains the transfer.
    store.fail_commit_calls.add(2)
    delivery = _DeliveryPort(store)
    processor = _processor(monkeypatch, store)

    async def _complete(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _complete)
    caplog.set_level("INFO", logger="okto_pulse.kg.consolidation_worker")
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    assert processed == 0
    assert store.entries[entry.id].status == "pending"
    assert all(
        getattr(record, "event", None)
        != "kg.stale_reconcile.delivery_transferred"
        for record in caplog.records
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["incomplete", "failed-types"])
async def test_non_complete_reconciliation_never_transfers_or_acknowledges(
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    entry = _entry()
    result = StaleReconcileResult(
        board_id=entry.board_id,
        correlation_id=entry.delete_event_id or "missing",
        incomplete=outcome == "incomplete",
        failed_types=["Requirement"] if outcome == "failed-types" else [],
    )
    store = _QueueStore(entry)
    delivery = _DeliveryPort(store)
    processor = _processor(monkeypatch, store)
    mark_failed_calls = _track_mark_failed(monkeypatch, processor)

    async def _process(*_args: object, **_kwargs: object) -> bool:
        return consolidation._stale_reconcile_is_complete(result)

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _process)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    fresh = store.entries[entry.id]
    assert processed == 0
    assert delivery.read_calls == []
    assert delivery.transfer_calls == []
    assert store.delivery_rows == {}
    assert store.ack_calls == []
    assert fresh.status == "pending"
    assert fresh.attempts == 1
    assert len(mark_failed_calls) == 1


@pytest.mark.asyncio
async def test_legacy_consolidate_keeps_standalone_ack_and_skips_delivery_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _entry(work_kind="consolidate")
    store = _QueueStore(entry)
    delivery = _DeliveryPort(store)
    processor = _processor(monkeypatch, store)
    mark_failed_calls = _track_mark_failed(monkeypatch, processor)

    async def _complete(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _complete)
    with _registered_ports(store, delivery):
        processed = await processor.process_batch()

    assert processed == 1
    assert delivery.read_calls == []
    assert delivery.transfer_calls == []
    assert store.delivery_rows == {}
    assert len(store.ack_calls) == 1
    assert store.ack_calls[0] == {
        "entry_id": entry.id,
        "claim_token": entry.claim_token,
        "generation": 0,
        "delete_event_id": None,
    }
    assert entry.id not in store.entries
    assert mark_failed_calls == []


class _WorkerCrash(BaseException):
    """Model abrupt cancellation/termination outside the Exception handler."""


@pytest.mark.asyncio
async def test_pre_transfer_base_exception_auto_rolls_back_and_preserves_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _entry()
    store = _QueueStore(entry)
    delivery = _DeliveryPort(store)
    processor = _processor(monkeypatch, store)
    mark_failed_calls = _track_mark_failed(monkeypatch, processor)

    async def _crash(
        _context: _Transaction,
        claimed: ConsolidationQueueRecord,
        **_kwargs: object,
    ) -> bool:
        store.partial_writes.add(claimed.id)
        store.entries[claimed.id].last_error = "uncommitted graph-adjacent write"
        raise _WorkerCrash("simulated process termination")

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _crash)
    with _registered_ports(store, delivery):
        with pytest.raises(_WorkerCrash, match="process termination"):
            await processor.process_batch()

    fresh = store.entries[entry.id]
    assert fresh.status == "claimed"
    assert fresh.claim_token
    assert fresh.last_error is None
    assert store.partial_writes == set()
    assert store.auto_rollback_count == 1
    assert store.rollback_count == 0
    assert delivery.transfer_calls == []
    assert store.ack_calls == []
    assert mark_failed_calls == []
