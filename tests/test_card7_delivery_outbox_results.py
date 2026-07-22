"""Card 7 — governed delivery attempt contracts and outbox outcomes."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest

from okto_pulse.core.application.processors import global_outbox as worker_module
from okto_pulse.core.application.processors.global_outbox import (
    DEAD_LETTER_SENTINEL,
    GlobalOutboxProcessor,
)
from okto_pulse.core.ports.delivery_ledger import (
    DELIVERY_OUTBOX_REASON,
    DELIVERY_REDRIVE_OUTBOX_REASON,
    DeliveryAttemptContractError,
    DeliveryAttemptEnvelope,
    DeliveryAttemptOutcome,
    parse_delivery_attempt_event,
)
from okto_pulse.core.ports.global_outbox import GlobalOutboxEventRecord


NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def _envelope(attempt: int = 0) -> DeliveryAttemptEnvelope:
    return DeliveryAttemptEnvelope(
        board_id="board-1",
        artifact_type="card",
        artifact_id="card-1",
        generation=3,
        delete_event_id="delete-1",
        attempt=attempt,
    )


def _event(*, attempt: int = 0, retry_count: int = 0) -> GlobalOutboxEventRecord:
    envelope = _envelope(attempt)
    return GlobalOutboxEventRecord(
        id=f"row-{attempt}",
        event_id=envelope.attempt_event_key,
        board_id=envelope.board_id,
        session_id=envelope.outbox_session_id,
        event_type=envelope.outbox_event_type,
        payload=dict(envelope.payload),
        retry_count=retry_count,
        last_error=None,
        processed_at=None,
        created_at=NOW,
    )


def test_attempt_envelope_derives_exclusive_reason_and_never_reused_key() -> None:
    initial = _envelope(0)
    redrive = _envelope(1)

    assert initial.reason == DELIVERY_OUTBOX_REASON
    assert redrive.reason == DELIVERY_REDRIVE_OUTBOX_REASON
    assert initial.attempt_event_key.endswith(":attempt:0")
    assert redrive.attempt_event_key.endswith(":attempt:1")
    assert initial.delivery_key == redrive.delivery_key
    assert initial.attempt_event_key != redrive.attempt_event_key
    assert dict(initial.payload)["event_id"] == initial.attempt_event_key
    assert parse_delivery_attempt_event(_event()) == initial


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "gd_parity:forged:attempt:0"),
        ("board_id", "board-other"),
        ("session_id", "session-other"),
        ("event_type", "other"),
        ("payload", {"delivery_key": "gd_parity:partial"}),
    ],
    ids=("event-id", "board-id", "session-id", "event-type", "payload"),
)
def test_attempt_parser_rejects_every_mutated_envelope(field: str, value: object) -> None:
    event = _event()
    setattr(event, field, value)

    with pytest.raises(DeliveryAttemptContractError):
        parse_delivery_attempt_event(event)


class _ClaimRepository:
    def __init__(self, events: list[GlobalOutboxEventRecord]) -> None:
        self.events = events

    async def claim_global_outbox(self, _context, *, limit: int):
        return self.events[:limit]


class _Store:
    def __init__(self) -> None:
        self.saved = False
        self.commit_calls = 0

    async def materialize_claimed(self, _context, claimed):
        return tuple(claimed)

    async def save_events(self, _context, _events) -> None:
        self.saved = True

    async def commit(self, _context) -> None:
        self.commit_calls += 1


class _DeliveryLedger:
    def __init__(self, store: _Store) -> None:
        self.store = store
        self.outcomes = []

    async def apply_attempt_outcomes(self, _context, outcomes) -> None:
        assert self.store.saved is True
        self.outcomes.extend(outcomes)


class _FailingDeliveryLedger(_DeliveryLedger):
    async def apply_attempt_outcomes(self, _context, outcomes) -> None:
        await super().apply_attempt_outcomes(_context, outcomes)
        raise RuntimeError("ledger outcome write failed")


@asynccontextmanager
async def _scope():
    yield object()


async def _run(
    monkeypatch: pytest.MonkeyPatch,
    event: GlobalOutboxEventRecord,
    *,
    apply_error: Exception | None = None,
    verification_error: Exception | None = None,
):
    store = _Store()
    ledger = _DeliveryLedger(store)
    monkeypatch.setattr(worker_module, "get_global_outbox_store", lambda: store)

    async def apply(_self, _event, _db):
        if apply_error is not None:
            raise apply_error
        return {}

    monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", apply)
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_verify_processed_batch",
        lambda _self, processed: (
            ({event.board_id: verification_error} if verification_error else {}),
            None,
        ),
    )
    processor = GlobalOutboxProcessor(
        _scope,
        claim_repository=_ClaimRepository([event]),
        delivery_ledger=ledger,
    )
    processed = await processor._process_once_under_writer()
    return processed, store, ledger


@pytest.mark.asyncio
async def test_verified_success_stages_delivered_before_same_commit(monkeypatch) -> None:
    event = _event()
    processed, store, ledger = await _run(monkeypatch, event)

    assert processed == 1
    assert store.commit_calls == 1
    assert event.processed_at is not None
    assert len(ledger.outcomes) == 1
    assert ledger.outcomes[0].outcome is DeliveryAttemptOutcome.DELIVERED
    assert ledger.outcomes[0].occurred_at == event.processed_at


@pytest.mark.asyncio
async def test_terminal_apply_failure_stages_debt_before_same_commit(monkeypatch) -> None:
    event = _event(attempt=2, retry_count=4)
    processed, store, ledger = await _run(
        monkeypatch,
        event,
        apply_error=RuntimeError("graph unavailable"),
    )

    assert processed == 0
    assert store.commit_calls == 1
    assert event.retry_count == DEAD_LETTER_SENTINEL
    assert len(ledger.outcomes) == 1
    assert ledger.outcomes[0].outcome is DeliveryAttemptOutcome.DELIVERY_DEBT
    assert "graph unavailable" in ledger.outcomes[0].error


@pytest.mark.asyncio
async def test_transient_failure_leaves_ledger_outbox_persisted(monkeypatch) -> None:
    event = _event(retry_count=0)
    processed, store, ledger = await _run(
        monkeypatch,
        event,
        apply_error=RuntimeError("temporary"),
    )

    assert processed == 0
    assert store.commit_calls == 1
    assert event.retry_count == 1
    assert ledger.outcomes == []


@pytest.mark.asyncio
async def test_post_flush_terminal_failure_never_marks_delivered(monkeypatch) -> None:
    event = _event(retry_count=4)
    processed, store, ledger = await _run(
        monkeypatch,
        event,
        verification_error=RuntimeError("digest mismatch"),
    )

    assert processed == 0
    assert store.commit_calls == 1
    assert event.processed_at is None
    assert event.retry_count == DEAD_LETTER_SENTINEL
    assert [item.outcome for item in ledger.outcomes] == [
        DeliveryAttemptOutcome.DELIVERY_DEBT
    ]


@pytest.mark.asyncio
async def test_outbox_commit_is_not_called_when_ledger_outcome_fails(
    monkeypatch,
) -> None:
    event = _event()
    store = _Store()
    ledger = _FailingDeliveryLedger(store)
    monkeypatch.setattr(worker_module, "get_global_outbox_store", lambda: store)

    async def apply(_self, _event, _db):
        return {}

    monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", apply)
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_verify_processed_batch",
        lambda _self, _processed: ({}, None),
    )
    processor = GlobalOutboxProcessor(
        _scope,
        claim_repository=_ClaimRepository([event]),
        delivery_ledger=ledger,
    )

    with pytest.raises(RuntimeError, match="ledger outcome write failed"):
        await processor._process_once_under_writer()

    assert store.saved is True
    assert store.commit_calls == 0
