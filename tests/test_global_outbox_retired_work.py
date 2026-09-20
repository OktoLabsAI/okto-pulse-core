"""Retired work cannot regain delivery ownership through old selections."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.processors import global_outbox as worker_module
from okto_pulse.core.application.global_outbox_dead_letter import GlobalOutboxDeadLetterError, GlobalOutboxDeadLetterOperations
from okto_pulse.core.ports.global_outbox import GLOBAL_OUTBOX_RETIRED_SENTINEL
from test_global_outbox_dead_letter_operations import MemoryGlobalOutboxStore, NOW, _row


@pytest.mark.asyncio
async def test_worker_ignores_retired_record_even_when_claim_adapter_returns_it(monkeypatch):
    event = _row("retired", offset=0, retry_count=GLOBAL_OUTBOX_RETIRED_SENTINEL)
    store = SimpleNamespace(materialize_claimed=AsyncMock(return_value=(event,)),
        save_events=AsyncMock(), commit=AsyncMock())
    context = object()
    @asynccontextmanager
    async def scope():
        yield context
    claims = SimpleNamespace(claim_global_outbox=AsyncMock(return_value=[event]))
    processor = worker_module.GlobalOutboxProcessor(scope, claim_repository=claims)
    processor._apply_event = AsyncMock(side_effect=AssertionError("retired work reached graph"))
    monkeypatch.setattr(worker_module, "get_global_outbox_store", lambda: store)
    assert await processor._process_once_under_writer() == 0
    processor._apply_event.assert_not_called()
    store.save_events.assert_awaited_once_with(context, [])
    assert event.processed_at is None
    assert event.retry_count == GLOBAL_OUTBOX_RETIRED_SENTINEL


@pytest.mark.asyncio
async def test_old_redrive_marker_does_not_make_retired_work_idempotently_queued():
    retired = _row("retired", offset=0, retry_count=GLOBAL_OUTBOX_RETIRED_SENTINEL,
        payload={"_dlq_reprocess": {"reason": "historical_retry"}})
    terminal = _row("terminal", offset=1)
    store = MemoryGlobalOutboxStore([retired, terminal])
    operations = GlobalOutboxDeadLetterOperations(store=store, clock=lambda: NOW)
    with pytest.raises(GlobalOutboxDeadLetterError) as error:
        await operations.reprocess(context=None, dead_letter_ids=["terminal", "retired"], reason="operator_retry")
    assert error.value.code == "mixed_selection_ineligible"
    assert terminal.retry_count == -1
    result = await operations.verify(context=None, dead_letter_ids=["retired"])
    assert result["items"][0]["state"] == "superseded"
    assert result["items"][0]["reason_code"] == "superseded_by_historical_migration"
    assert result["items"][0]["authoritative_id"] is None
