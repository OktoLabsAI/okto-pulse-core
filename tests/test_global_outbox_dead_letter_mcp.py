from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.global_outbox_dead_letter import (
    GlobalOutboxDeadLetterError,
)
from okto_pulse.core.mcp import server


class FakeKGOperations:
    async def list_global_outbox_dead_letters(
        self, *, limit: int, cursor: str | None, classification: str | None
    ):
        assert (limit, cursor, classification) == (2, None, None)
        return {
            "items": [{"dead_letter_id": "dlq-a", "state": "terminal"}],
            "next_cursor": "cursor-2",
            "count": 1,
        }

    async def reprocess_global_outbox_dead_letters(
        self, *, dead_letter_ids: list[str], reason: str
    ):
        assert dead_letter_ids == ["dlq-a"]
        assert reason == "operator_retry_after_graph_recovery"
        return {
            "selected_ids": ["dlq-a"],
            "requeued_ids": ["dlq-a"],
            "already_queued_ids": [],
            "already_applied_ids": [],
            "rejected_ids": [],
        }

    async def verify_global_outbox_dead_letters(self, *, dead_letter_ids: list[str]):
        assert dead_letter_ids == ["dlq-a", "missing"]
        return {
            "items": [
                {"dead_letter_id": "dlq-a", "state": "queued"},
                {"dead_letter_id": "missing", "state": "absent"},
            ]
        }


class FakeUow:
    def __init__(self, kg: object) -> None:
        self.services = SimpleNamespace(kg=kg)
        self.commits = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def commit(self) -> None:
        self.commits += 1


class RecordingFactory:
    def __init__(self, kg: object) -> None:
        self.kg = kg
        self.instances: list[FakeUow] = []

    def __call__(self, **_kwargs) -> FakeUow:
        uow = FakeUow(self.kg)
        self.instances.append(uow)
        return uow


async def _authorized():
    return SimpleNamespace(agent_id="dlq-operator"), None


@pytest.mark.asyncio
@pytest.mark.parametrize("_repeat", range(10))
async def test_actual_fastmcp_tools_use_one_dedicated_uow_per_operation_and_signal(
    _repeat: int,
    monkeypatch: pytest.MonkeyPatch,
):
    from okto_pulse.core.application import runtime_workers

    factory = RecordingFactory(FakeKGOperations())
    signals: list[str] = []
    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", lambda: factory)
    monkeypatch.setattr(
        runtime_workers,
        "signal_runtime_worker",
        lambda family: signals.append(family) or True,
    )

    list_tool = await server.mcp.get_tool(
        "okto_pulse_kg_global_outbox_dead_letter_list"
    )
    reprocess_tool = await server.mcp.get_tool(
        "okto_pulse_kg_global_outbox_dead_letter_reprocess"
    )
    verify_tool = await server.mcp.get_tool(
        "okto_pulse_kg_global_outbox_dead_letter_verify"
    )

    listed = json.loads(await list_tool.fn(limit=2, cursor=None, classification=None))
    reprocessed = json.loads(
        await reprocess_tool.fn(
            dead_letter_ids=["dlq-a"],
            reason="operator_retry_after_graph_recovery",
            process_now=True,
        )
    )
    verified = json.loads(await verify_tool.fn(dead_letter_ids=["dlq-a", "missing"]))

    assert listed["next_cursor"] == "cursor-2"
    assert reprocessed["requeued_ids"] == ["dlq-a"]
    assert reprocessed["worker_signaled"] is True
    assert verified["items"][1]["state"] == "absent"
    assert len(factory.instances) == 3
    assert [uow.commits for uow in factory.instances] == [0, 1, 0]
    assert signals == ["outbox_worker"]
    assert list_tool.parameters["properties"]["limit"]["maximum"] == 100
    assert reprocess_tool.parameters["required"] == [
        "dead_letter_ids",
        "reason",
    ]


@pytest.mark.asyncio
async def test_actual_fastmcp_reprocess_returns_typed_fail_closed_errors(
    monkeypatch: pytest.MonkeyPatch,
):
    class RejectingKG(FakeKGOperations):
        async def reprocess_global_outbox_dead_letters(self, **_kwargs):
            raise GlobalOutboxDeadLetterError(
                "mixed_selection_ineligible",
                detail={"rejected_count": 1},
            )

    factory = RecordingFactory(RejectingKG())
    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", lambda: factory)

    payload = json.loads(
        await server.okto_pulse_kg_global_outbox_dead_letter_reprocess.fn(
            dead_letter_ids=["dlq-a", "wrong-class"],
            reason="operator_retry_after_graph_recovery",
            process_now=False,
        )
    )

    assert payload == {
        "error": "mixed_selection_ineligible",
        "mutated": False,
        "rejected_count": 1,
    }
    assert len(factory.instances) == 1
    assert factory.instances[0].commits == 0


@pytest.mark.asyncio
async def test_actual_fastmcp_maps_sqlite_busy_without_leaking_backend_details(
    monkeypatch: pytest.MonkeyPatch,
):
    class OperationalError(Exception):
        pass

    class BusyKG(FakeKGOperations):
        async def reprocess_global_outbox_dead_letters(self, **_kwargs):
            raise OperationalError(
                "sqlite3.OperationalError: database is locked at C:/secret/pulse.db"
            )

    factory = RecordingFactory(BusyKG())
    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", lambda: factory)

    payload = json.loads(
        await server.okto_pulse_kg_global_outbox_dead_letter_reprocess.fn(
            dead_letter_ids=["dlq-a"],
            reason="operator_retry_after_graph_recovery",
            process_now=False,
        )
    )

    assert payload == {"error": "global_outbox_busy", "mutated": False}
    assert "secret" not in json.dumps(payload)
