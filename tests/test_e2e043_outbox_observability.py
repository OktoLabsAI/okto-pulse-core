"""Operational observations must follow durable outcomes, never declare them."""

import asyncio
import hashlib
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.processors import global_outbox as worker
from okto_pulse.core.kg.global_discovery_writer import (
    GlobalDiscoveryWriterContention,
    GlobalDiscoveryWriterLease,
)
from okto_pulse.core.kg.interfaces.graph_errors import GraphUnavailable
from test_global_outbox_dead_letter_operations import _row
from test_card7_delivery_outbox_results import _event


def _records(caplog):
    return [
        r for r in caplog.records if getattr(r, "event", None) == "outbox.operation"
    ]


@pytest.fixture
def harness(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger=worker.logger.name)
    order = []
    event = _row(
        "row-1",
        offset=0,
        retry_count=0,
        last_error=None,
        payload={"private_content": "AUTHORED-SECRET"},
    )

    class Session:
        async def rollback(self):
            order.append("read_released")

    @asynccontextmanager
    async def scope():
        yield Session()

    class Lease:
        owner_token = "PRIVATE-LEASE-TOKEN"
        operation = "test_outbox_observability"
        released = False

        # Keep the unit lease, but use the production context propagation so
        # acknowledgement-boundary checks observe the same active owner.
        guard = GlobalDiscoveryWriterLease.guard

        def assert_fenced(self):
            pass

        def renew(self):
            pass

        def release(self):
            self.released = True

    lease = Lease()
    monkeypatch.setattr(GlobalDiscoveryWriterLease, "acquire", lambda **kw: lease)
    claims = SimpleNamespace(claim_global_outbox=AsyncMock(return_value=[event]))
    store = SimpleNamespace(
        materialize_claimed=AsyncMock(return_value=(event,)),
        save_events=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(worker, "get_global_outbox_store", lambda: store)
    processor = worker.GlobalOutboxProcessor(scope, claim_repository=claims)

    async def apply(event, db):
        assert order == ["read_released"]
        order.append("graph")
        return {}

    async def commit(db):
        assert not [r for r in _records(caplog) if r.stage == "ack_result"]
        order.append("commit")

    store.commit.side_effect = commit
    processor._apply_event = AsyncMock(side_effect=apply)
    processor._verify_processed_batch = lambda events: ({}, None)
    return SimpleNamespace(
        processor=processor,
        event=event,
        store=store,
        claims=claims,
        lease=lease,
        order=order,
    )


@pytest.mark.asyncio
async def test_observations_correlate_phases_only_ack_after_commit(harness, caplog):
    assert await harness.processor.process_once() == 1
    rows = _records(caplog)
    assert [r.stage for r in rows] == [
        "lease_acquire",
        "sql_claim",
        "graph_apply",
        "graph_verify",
        "sql_ack",
        "ack_result",
        "lease_release",
        "batch",
    ]
    assert len({r.batch_id for r in rows}) == 1
    assert all(r.elapsed_ms >= 0 for r in rows if r.stage != "ack_result")
    assert all(
        r.lease_ref == hashlib.sha256(b"PRIVATE-LEASE-TOKEN").hexdigest() for r in rows
    )
    ack = next(r for r in rows if r.stage == "ack_result")
    assert (ack.event_id, ack.claim_row_id, ack.board_id, ack.ack_state) == (
        harness.event.event_id,
        harness.event.id,
        harness.event.board_id,
        "delivered",
    )
    assert ack.delivery_attempt is None
    assert harness.order == ["read_released", "graph", "commit"]
    assert harness.lease.released
    assert worker._observation_context.get() is None
    assert "AUTHORED-SECRET" not in str([r.__dict__ for r in rows])
    assert "PRIVATE-LEASE-TOKEN" not in str([r.__dict__ for r in rows])


@pytest.mark.parametrize(
    "failure", [RuntimeError("SQL-SECRET"), asyncio.CancelledError()]
)
@pytest.mark.asyncio
async def test_failed_or_cancelled_ack_never_logs_delivered(harness, caplog, failure):
    harness.store.commit.side_effect = failure
    with pytest.raises(type(failure)):
        await harness.processor.process_once()
    rows = _records(caplog)
    assert not any(r.stage == "ack_result" for r in rows)
    ack = next(r for r in rows if r.stage == "sql_ack")
    assert ack.ack_state == "unconfirmed"
    assert ack.error_code in {"operation_cancelled", "operation_failed"}
    assert harness.lease.released
    assert "SQL-SECRET" not in str([r.__dict__ for r in rows])


@pytest.mark.parametrize("verify", [False, True])
@pytest.mark.asyncio
async def test_graph_failure_records_pending_not_delivery_and_redacts(
    harness, caplog, verify
):
    error = GraphUnavailable("GRAPH-SECRET", details={"query": "AUTHORED-SECRET"})
    if verify:
        harness.processor._verify_processed_batch = lambda events: ({}, error)
    else:
        harness.processor._apply_event.side_effect = error
    assert await harness.processor.process_once() == 0
    rows = _records(caplog)
    phase = next(
        r for r in rows if r.stage == ("graph_verify" if verify else "graph_apply")
    )
    assert phase.outcome == "error"
    assert phase.error_code == "graph_unavailable"
    ack = next(r for r in rows if r.stage == "ack_result")
    assert ack.ack_state == "retry_pending"
    assert ack.retry_count == 1
    assert "GRAPH-SECRET" not in str([r.__dict__ for r in rows])


@pytest.mark.asyncio
async def test_contention_does_not_touch_relational_claim(harness, caplog, monkeypatch):
    def contend(**kw):
        raise GlobalDiscoveryWriterContention("PRIVATE-CURRENT-OWNER")

    monkeypatch.setattr(GlobalDiscoveryWriterLease, "acquire", contend)
    assert await harness.processor.process_once() == 0
    harness.claims.claim_global_outbox.assert_not_awaited()
    rows = _records(caplog)
    acquire = next(r for r in rows if r.stage == "lease_acquire")
    assert acquire.outcome == "contended"
    assert acquire.error_code == "writer_lease_contended"
    assert acquire.lease_acquire_attempts == 1
    assert acquire.elapsed_ms >= 0
    assert "PRIVATE-CURRENT-OWNER" not in str([r.__dict__ for r in rows])


@pytest.mark.asyncio
async def test_governed_attempt_identity_and_terminal_history_remain_distinct(
    harness, caplog
):
    event = _event(attempt=2, retry_count=4)
    harness.store.materialize_claimed.return_value = (event,)
    harness.processor._apply_event.side_effect = GraphUnavailable("hidden")
    ledger = SimpleNamespace(apply_attempt_outcomes=AsyncMock())
    harness.processor._delivery_ledger = ledger
    assert await harness.processor.process_once() == 0
    ack = next(r for r in _records(caplog) if r.stage == "ack_result")
    assert ack.delivery_attempt == 2
    assert ack.event_id == event.event_id
    assert ack.retry_count == -1
    assert ack.ack_state == "terminal"
    ledger.apply_attempt_outcomes.assert_awaited_once()
    outcome = ledger.apply_attempt_outcomes.await_args.args[1][0]
    assert outcome.envelope.attempt == 2
    assert outcome.outcome.value == "delivery_debt"
    assert event.processed_at is None


@pytest.mark.asyncio
async def test_claim_adapter_cannot_expand_bounded_processing_or_logging(
    harness, caplog
):
    harness.store.materialize_claimed.return_value = tuple(
        _row(str(i), offset=i, retry_count=0) for i in range(51)
    )
    with pytest.raises(ValueError, match="claim_bound_exceeded"):
        await harness.processor.process_once()
    harness.processor._apply_event.assert_not_awaited()
    harness.store.commit.assert_not_awaited()
    assert not any(r.stage == "ack_result" for r in _records(caplog))


def test_untrusted_identity_and_error_codes_are_not_logged_as_content():
    event = _row("row\nSECRET" + "x" * 300, offset=0)
    fields = worker._event_observation(event)
    assert fields["claim_row_id"].startswith("sha256:")
    assert (
        max(map(len, [fields["claim_row_id"], fields["event_id"], fields["board_id"]]))
        <= 128
    )
    error = GraphUnavailable("hidden")
    error.code = "SECRET"
    assert worker._safe_error_code(error) == "operation_failed"
