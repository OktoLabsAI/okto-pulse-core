"""Native outbox counters reject invalid state without converting it."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from okto_pulse.core.application.processors import global_outbox as worker_module
from test_global_outbox_dead_letter_operations import _row


@pytest.mark.parametrize("value", [-2, -999, None, True, 1.5, "1"])
def test_invalid_retry_count_is_not_a_second_terminal_format(value):
    with pytest.raises(ValueError, match="global_outbox_retry_count_invalid"):
        _row("invalid", offset=0, retry_count=value)


@pytest.mark.asyncio
async def test_invalid_claim_refuses_whole_batch_before_graph_or_save(monkeypatch):
    valid = _row("current", offset=0, retry_count=0)
    invalid = _row("invalid", offset=1, retry_count=0)
    invalid.retry_count = -2
    store = SimpleNamespace(materialize_claimed=AsyncMock(return_value=(valid, invalid)),
        save_events=AsyncMock(), commit=AsyncMock())
    context = object()

    @asynccontextmanager
    async def scope():
        yield context

    claims = SimpleNamespace(claim_global_outbox=AsyncMock(return_value=[valid, invalid]))
    processor = worker_module.GlobalOutboxProcessor(scope, claim_repository=claims)
    processor._apply_event = AsyncMock(side_effect=AssertionError("invalid batch reached graph"))
    monkeypatch.setattr(worker_module, "get_global_outbox_store", lambda: store)
    with pytest.raises(ValueError, match="global_outbox_retry_count_invalid"):
        await processor._process_once_under_writer()
    processor._apply_event.assert_not_called()
    store.save_events.assert_not_called()
    store.commit.assert_not_called()
    assert valid.retry_count == 0
    assert invalid.retry_count == -2


@pytest.mark.asyncio
async def test_claim_read_checkout_is_released_before_graph_work(monkeypatch):
    event = _row("claim-checkout", offset=0, retry_count=0)
    order = []

    class Session:
        async def rollback(self):
            order.append("release_read_checkout")

    @asynccontextmanager
    async def scope():
        yield Session()

    async def apply(_event, _db):
        assert order == ["release_read_checkout"]
        order.append("graph_apply")
        return {}

    async def run_graph_io(operation):
        return operation()

    async def save(_db, _events):
        order.append("save")

    async def commit(_db):
        order.append("commit")

    store = SimpleNamespace(
        materialize_claimed=AsyncMock(return_value=(event,)),
        save_events=save,
        commit=commit,
    )
    claims = SimpleNamespace(claim_global_outbox=AsyncMock(return_value=[event]))
    processor = worker_module.GlobalOutboxProcessor(scope, claim_repository=claims)
    processor._apply_event = apply
    processor._run_graph_io = run_graph_io
    processor._verify_processed_batch = lambda _processed: ({}, None)
    monkeypatch.setattr(worker_module, "get_global_outbox_store", lambda: store)

    assert await processor._process_once_under_writer() == 1
    assert order == ["release_read_checkout", "graph_apply", "save", "commit"]


@pytest.mark.asyncio
async def test_sqlite_claim_transaction_ends_before_graph_work(db_factory, monkeypatch):
    event = _row("sqlite-claim-checkout", offset=0, retry_count=0)

    async def claim(db, *, limit):
        assert limit > 0
        await db.execute(text("SELECT 1"))
        assert db.in_transaction()
        return [event]

    async def apply(_event, db):
        assert not db.in_transaction()
        return {}

    async def run_graph_io(operation):
        return operation()

    store = SimpleNamespace(
        materialize_claimed=AsyncMock(return_value=(event,)),
        save_events=AsyncMock(),
        commit=AsyncMock(),
    )
    processor = worker_module.GlobalOutboxProcessor(
        db_factory,
        claim_repository=SimpleNamespace(claim_global_outbox=claim),
    )
    processor._apply_event = apply
    processor._run_graph_io = run_graph_io
    processor._verify_processed_batch = lambda _processed: ({}, None)
    monkeypatch.setattr(worker_module, "get_global_outbox_store", lambda: store)

    assert await processor._process_once_under_writer() == 1
    store.commit.assert_awaited_once()
