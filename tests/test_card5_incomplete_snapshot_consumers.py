"""Card 5: every board-source consumer fails closed on an incomplete census."""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.application.kg_rebuild import build_source_store
from okto_pulse.core.application.rebuild_ports import BoardSourceSnapshot
from okto_pulse.core.kg import canonical_debt_replay
from okto_pulse.core.kg import provenance_drift
from okto_pulse.core.kg.interfaces import SourceUnavailableError
from okto_pulse.core.services import kg_health_service
from sqlalchemy_test_models import Board


class _IncompleteBoardSourceReader:
    def __init__(self) -> None:
        self.fetch_calls: list[str] = []

    def fetch(self, board_id: str) -> BoardSourceSnapshot:
        self.fetch_calls.append(board_id)
        return BoardSourceSnapshot(complete=False, cause="table_missing")


@pytest.fixture
def incomplete_reader(_kg_registry_test_fakes) -> _IncompleteBoardSourceReader:
    reader = _IncompleteBoardSourceReader()
    configure_test_kg_registry(board_source_reader=reader)
    return reader


def test_rebuild_source_store_rejects_incomplete_snapshot(
    incomplete_reader: _IncompleteBoardSourceReader,
) -> None:
    source_store = build_source_store()

    with pytest.raises(SourceUnavailableError) as raised:
        source_store("board-rebuild")

    assert raised.value.cause_type == "table_missing"
    assert incomplete_reader.fetch_calls == ["board-rebuild"]


@pytest.mark.asyncio
async def test_canonical_debt_replay_rejects_incomplete_snapshot_before_commit(
    incomplete_reader: _IncompleteBoardSourceReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _one_open_debt(*_args, **_kwargs):
        return SimpleNamespace(
            items=[
                {
                    "canonical_state": "pending",
                    "artifact_id": "spec-1",
                    "source_ref": "spec:spec-1",
                    "content_hash": "hash-1",
                }
            ]
        )

    async def _must_not_reconcile(*_args, **_kwargs):
        raise AssertionError("incomplete source census reached debt reconciliation")

    monkeypatch.setattr(
        canonical_debt_replay,
        "list_canonical_debt",
        _one_open_debt,
    )
    monkeypatch.setattr(
        canonical_debt_replay,
        "reconcile_canonical_debt_with_evidence",
        _must_not_reconcile,
    )

    with pytest.raises(SourceUnavailableError) as raised:
        await canonical_debt_replay.replay_canonical_debt_by_maturity(
            object(),
            board_id="board-debt",
        )

    assert raised.value.cause_type == "table_missing"
    assert incomplete_reader.fetch_calls == ["board-debt"]


@pytest.mark.asyncio
async def test_provenance_drift_rejects_incomplete_snapshot_before_graph_access(
    incomplete_reader: _IncompleteBoardSourceReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _must_not_read_graph(*_args, **_kwargs):
        raise AssertionError("incomplete source census reached graph access")

    monkeypatch.setattr(
        provenance_drift,
        "_fetch_provenance_nodes",
        _must_not_read_graph,
    )

    with pytest.raises(SourceUnavailableError) as raised:
        await provenance_drift.provenance_drift_report("board-drift")

    assert raised.value.cause_type == "table_missing"
    assert incomplete_reader.fetch_calls == ["board-drift"]


def test_kg_health_source_diagnostic_reports_enumeration_failure_not_zero(
    incomplete_reader: _IncompleteBoardSourceReader,
) -> None:
    diagnostic = kg_health_service._probe_rebuild_source_diagnostics(
        "board-health-diagnostic"
    )

    assert diagnostic["enumeration_failure"] is True
    assert diagnostic["source_count"] is None
    assert diagnostic["canonical_source_count"] is None
    assert diagnostic["working_source_count"] is None
    assert "SourceUnavailableError" in diagnostic["error"]
    assert "table_missing" in diagnostic["error"]
    assert incomplete_reader.fetch_calls == ["board-health-diagnostic"]


@pytest.mark.asyncio
async def test_kg_health_cannot_report_healthy_for_incomplete_source_snapshot(
    incomplete_reader: _IncompleteBoardSourceReader,
    db_factory,
) -> None:
    board_id = f"card5-incomplete-{uuid.uuid4().hex[:10]}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Card 5 incomplete", owner_id="owner"))
        await db.commit()

    async with db_factory() as db:
        health = await kg_health_service.get_kg_health(board_id, db)

    source_failure = health["root_cause"]["categories"][
        "source_enumeration_failure"
    ]
    assert source_failure["present"] is True
    assert health["root_cause"]["drilldown_unavailable"] is True
    assert health["source_count"] is None
    assert health["overall_state"] != "healthy"
    assert incomplete_reader.fetch_calls
    assert set(incomplete_reader.fetch_calls) == {board_id}
