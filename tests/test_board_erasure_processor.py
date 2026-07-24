"""Durable board-erasure worker redrive."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest

from okto_pulse.core.ports.kg_governance import BoardErasureJobFact


class _Store:
    def __init__(self, job: BoardErasureJobFact) -> None:
        self.job = job
        self.commits = 0
        self.completed = False

    async def list_due_board_erasure_jobs(self, _db, *, now, limit):
        assert now.tzinfo is not None
        assert limit == 10
        return () if self.completed else (self.job,)

    async def get_board_erasure_job(self, _db, *, board_id):
        assert board_id == self.job.board_id
        return None if self.completed else self.job

    async def complete_board_erasure_job(self, _db, *, board_id):
        assert board_id == self.job.board_id
        self.completed = True
        return True

    async def commit(self, _db):
        self.commits += 1


class _Lease:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def ensure_owned(self) -> None:
        self.events.append("ensure")


@pytest.mark.asyncio
async def test_board_erasure_processor_completes_a_persisted_job(monkeypatch) -> None:
    from okto_pulse.core.application.processors import board_erasure as module

    job = BoardErasureJobFact(
        board_id="board-redrive",
        actor_id="owner",
        attempts=2,
        last_error="transient",
        next_attempt_at=datetime.now(timezone.utc),
    )
    store = _Store(job)
    events: list[str] = []

    @asynccontextmanager
    async def _relational_scope():
        yield object()

    @asynccontextmanager
    async def _erasure_scope(board_id, *, actor_id):
        assert board_id == job.board_id
        assert actor_id == "system:board-erasure-worker"
        events.append("enter")
        yield _Lease(events)
        events.append("exit")

    async def _erase(_db, board_id, **kwargs):
        assert board_id == job.board_id
        assert kwargs == {
            "strict": True,
            "commit": False,
            "global_writer_guarded": True,
            "purge_relational": False,
        }
        events.append("erase")

    monkeypatch.setattr(module, "get_kg_governance_store", lambda: store)
    monkeypatch.setattr(module, "board_erasure_scope", _erasure_scope)
    monkeypatch.setattr(module, "right_to_erasure", _erase)

    processor = module.BoardErasureProcessor(_relational_scope)
    assert await processor.process_once() == 1
    assert store.completed is True
    assert store.commits == 1
    assert events == ["enter", "erase", "ensure", "exit"]


@pytest.mark.asyncio
async def test_board_erasure_processor_keeps_job_after_external_failure(
    monkeypatch,
) -> None:
    from okto_pulse.core.application.processors import board_erasure as module

    job = BoardErasureJobFact(
        board_id="board-redrive-failure",
        actor_id="owner",
        attempts=0,
        last_error=None,
        next_attempt_at=datetime.now(timezone.utc),
    )
    store = _Store(job)
    recorded: list[tuple[str, str]] = []

    @asynccontextmanager
    async def _relational_scope():
        yield object()

    @asynccontextmanager
    async def _erasure_scope(*_args, **_kwargs):
        yield _Lease([])

    async def _erase(*_args, **_kwargs):
        raise RuntimeError("storage unavailable")

    async def _record(_db, board_id, error):
        recorded.append((board_id, str(error)))

    monkeypatch.setattr(module, "get_kg_governance_store", lambda: store)
    monkeypatch.setattr(module, "board_erasure_scope", _erasure_scope)
    monkeypatch.setattr(module, "right_to_erasure", _erase)
    monkeypatch.setattr(module, "record_board_erasure_failure", _record)

    processor = module.BoardErasureProcessor(_relational_scope)
    assert await processor.process_once() == 0
    assert store.completed is False
    assert store.commits == 1
    assert recorded == [(job.board_id, "storage unavailable")]
