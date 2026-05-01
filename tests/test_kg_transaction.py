from __future__ import annotations

import pytest

from okto_pulse.core.kg.transaction import TransactionOrchestrator


class _FakeResult:
    def __init__(self, has_row: bool):
        self.has_row = has_row
        self.closed = False

    def has_next(self) -> bool:
        return self.has_row

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, result: _FakeResult):
        self.result = result
        self.statements: list[tuple[str, dict]] = []

    def execute(self, statement: str, params: dict):
        self.statements.append((statement, params))
        return self.result


def test_create_edge_requires_materialized_relationship():
    result = _FakeResult(has_row=False)
    conn = _FakeConnection(result)
    orch = TransactionOrchestrator(
        conn,
        sqlite_session=None,  # type: ignore[arg-type]
        session_id="sess-edge",
        board_id="board-edge",
    )

    with pytest.raises(ValueError, match="endpoint nodes were not matched"):
        orch.create_edge("supersedes", "missing-source", "missing-target")

    assert orch.counters.edges_added == 0
    assert orch.records == []
    assert result.closed is True
    assert "RETURN r.created_by_session_id" in conn.statements[0][0]


def test_create_edge_counts_only_confirmed_relationship():
    result = _FakeResult(has_row=True)
    conn = _FakeConnection(result)
    orch = TransactionOrchestrator(
        conn,
        sqlite_session=None,  # type: ignore[arg-type]
        session_id="sess-edge",
        board_id="board-edge",
    )

    orch.create_edge("supersedes", "decision-new", "decision-old")

    assert orch.counters.edges_added == 1
    assert len(orch.records) == 1
    assert result.closed is True
