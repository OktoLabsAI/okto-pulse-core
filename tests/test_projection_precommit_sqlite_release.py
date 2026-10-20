"""A failed projection prepare must not hold the SQLite writer during graph I/O."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import consolidation


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["prepare", "claim_lost", "commit"])
async def test_precommit_failure_releases_sqlite_writer_before_graph_barrier(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    events: list[str] = []
    db = SimpleNamespace(writer_open=False)

    class Store:
        async def rollback(self, _db):
            events.append("rollback")
            db.writer_open = False

        async def prepare_projection_operation(self, _db, **_kwargs):
            events.append("prepare")
            db.writer_open = True
            if failure == "prepare":
                raise RuntimeError("injected_prepare_failure")
            return failure != "claim_lost"

        async def commit(self, _db):
            events.append("commit")
            if failure == "commit":
                raise RuntimeError("injected_commit_failure")
            db.writer_open = False

    async def fake_commit(_request, **kwargs):
        await kwargs["projection_precommit"](
            {"graph_after": "native-baseline", "output": {}}
        )
        raise AssertionError("precommit unexpectedly succeeded")

    async def fake_barrier(**_kwargs):
        assert db.writer_open is False
        events.append("graph_barrier")

    monkeypatch.setattr(consolidation, "get_consolidation_persistence_port", Store)
    monkeypatch.setattr(consolidation, "commit_consolidation", fake_commit)
    monkeypatch.setattr(consolidation, "_ensure_board_graph_durable", fake_barrier)

    entry = SimpleNamespace(
        id="entry", board_id="board", artifact_type="spec", artifact_id="spec",
        claim_token="claim", source="state_transition",
    )
    with pytest.raises(Exception, match="injected_|projection_precommit_claim_lost"):
        await consolidation._commit_consolidation_with_board_graph_lifecycle(
            entry=entry,
            session_id="session",
            summary_text="summary",
            db=db,
            enter_graph_write=lambda _mutation_ref: object(),
            journaled_prepared=[],
            journaled_receipts=[],
        )

    assert events[0:2] == ["rollback", "prepare"]
    assert events[-2:] == ["rollback", "graph_barrier"]
