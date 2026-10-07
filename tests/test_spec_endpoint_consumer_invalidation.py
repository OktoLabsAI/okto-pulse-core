"""Endpoint refresh is bounded and staged through public relational ports."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.processors.consolidation import _enqueue_superseded_spec_consumers
from okto_pulse.core.ports import application_persistence, relational_effects


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_endpoint_consumers_are_scoped_deduplicated_and_preserve_active_claims(monkeypatch, empty):
    context = object()
    queries = []

    async def read(db, query):
        assert db is context
        queries.append(query)
        filters = {(item.field, item.operator): item.value for item in query.filters}
        assert filters[("board_id", "eq")] == "board"
        if len(queries) == 1:
            assert filters == {("board_id", "eq"): "board", ("spec_id", "eq"): "spec"}
            return () if empty else (SimpleNamespace(id="card"), SimpleNamespace(id="existing-bug"))
        assert filters[("card_type", "eq")] == "bug"
        assert filters[("origin_task_id", "in")] == ("card", "existing-bug")
        return (SimpleNamespace(id="existing-bug"), SimpleNamespace(id="proxy-bug"))

    write = AsyncMock(return_value=True)
    monkeypatch.setattr(application_persistence, "get_application_persistence_port",
        lambda: SimpleNamespace(list=read))
    monkeypatch.setattr(relational_effects, "get_relational_effects_port",
        lambda: SimpleNamespace(upsert_consolidation_queue_unless_tombstoned=write))
    assert await _enqueue_superseded_spec_consumers(context, board_id="board", spec_id="spec") == (0 if empty else 3)
    assert len(queries) == (1 if empty else 2)
    requests = [call.args[1] for call in write.await_args_list]
    assert [item.artifact_id for item in requests] == ([] if empty else ["card", "existing-bug", "proxy-bug"])
    assert all(item.board_id == "board" and item.artifact_type == "card"
        and item.coalesce_active and item.source == "projection:spec_endpoint_superseded" for item in requests)


@pytest.mark.asyncio
async def test_notification_failure_propagates_to_the_caller_uow(monkeypatch):
    read = AsyncMock(side_effect=[(SimpleNamespace(id="card"),), ()])
    write = AsyncMock(side_effect=RuntimeError("notification_failed"))
    monkeypatch.setattr(application_persistence, "get_application_persistence_port",
        lambda: SimpleNamespace(list=read))
    monkeypatch.setattr(relational_effects, "get_relational_effects_port",
        lambda: SimpleNamespace(upsert_consolidation_queue_unless_tombstoned=write))
    with pytest.raises(RuntimeError, match="notification_failed"):
        await _enqueue_superseded_spec_consumers(object(), board_id="board", spec_id="spec")
