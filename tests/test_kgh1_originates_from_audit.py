"""KGH-1 — advisory read-only audit for persisted originates_from edges."""

from __future__ import annotations

import pytest

from okto_pulse.core.application.use_cases import ActorContext
from okto_pulse.core.application.use_cases.mcp_kg_crud import (
    AuditOriginatesFromContractCommand,
    AuditOriginatesFromContractUseCase,
)
from okto_pulse.core.kg.originates_from_audit import (
    CLASSIFICATION_INVALID,
    CLASSIFICATION_OK,
    CLASSIFICATION_UNKNOWN,
    audit_originates_from_contract,
)
from okto_pulse.core.kg.primitives import KGPrimitiveError, _validate_local_edge_pair


class _AuditExecutor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []
        self.mutation_calls = []

    def execute_read_only(self, board_id, cypher, params=None, *, max_rows=1000):
        self.calls.append(
            {
                "board_id": board_id,
                "cypher": cypher,
                "params": dict(params or {}),
                "max_rows": max_rows,
            }
        )
        return {
            "rows": self.rows,
            "row_count": len(self.rows),
            "truncated": False,
        }

    def execute(self, *args, **kwargs):  # pragma: no cover - must never be called
        self.mutation_calls.append((args, kwargs))
        raise AssertionError("audit must use execute_read_only only")

    def is_supported(self):
        return True


class _UnsupportedExecutor(_AuditExecutor):
    def is_supported(self):
        return False


def test_kgh1_audit_lists_invalid_and_unknown_without_ok_by_default():
    executor = _AuditExecutor(
        [
            ["rel-invalid", "agent", "bug-1", "bug-2", "Bug", "Bug", "legacy"],
            ["rel-unknown", "agent", "existing-kg:bug", "target-1", "Bug", None, "legacy"],
            ["rel-ok", "agent", "bug-3", "entity-1", "Bug", "Entity", "canonical"],
        ]
    )

    report = audit_originates_from_contract(
        "board-kgh1",
        cypher_executor=executor,
    )

    assert report["read_only"] is True
    assert report["mutated"] is False
    assert report["status"] == "advisory_findings"
    assert report["counts"] == {
        CLASSIFICATION_INVALID: 1,
        CLASSIFICATION_UNKNOWN: 1,
        CLASSIFICATION_OK: 1,
    }
    assert report["total"] == 2
    assert [item["relationship_id"] for item in report["items"]] == [
        "rel-invalid",
        "rel-unknown",
    ]
    assert report["items"][0]["classification"] == CLASSIFICATION_INVALID
    assert report["items"][0]["confidence"] == "high"
    assert "invalid_edge_endpoint_types" in report["items"][0]["reason"]
    assert report["items"][1]["classification"] == CLASSIFICATION_UNKNOWN
    assert report["items"][1]["confidence"] == "low"
    assert executor.mutation_calls == []


def test_kgh1_audit_include_ok_keeps_ok_edges_advisory_only():
    executor = _AuditExecutor(
        [["rel-ok", "agent", "bug-3", "entity-1", "Bug", "Entity", "canonical"]]
    )

    report = audit_originates_from_contract(
        "board-kgh1",
        include_ok=True,
        cypher_executor=executor,
    )

    assert report["total"] == 1
    assert report["items"][0]["classification"] == CLASSIFICATION_OK
    assert report["items"][0]["mutated"] is False


def test_kgh1_audit_degrades_when_read_port_is_unavailable():
    report = audit_originates_from_contract(
        "board-kgh1",
        cypher_executor=_UnsupportedExecutor([]),
    )

    assert report["status"] == "unavailable"
    assert report["reason"] == "cypher_executor_unavailable"
    assert report["items"] == []
    assert report["mutated"] is False


def test_kgh1_audit_uses_read_only_cypher_only():
    executor = _AuditExecutor([])

    audit_originates_from_contract("board-kgh1", cypher_executor=executor)

    assert len(executor.calls) == 1
    cypher = executor.calls[0]["cypher"].upper()
    assert "MATCH" in cypher
    forbidden_write_tokens = (" CREATE ", " MERGE ", " SET ", " DELETE ", " DETACH ")
    assert not any(token in f" {cypher} " for token in forbidden_write_tokens)


def test_kgh1_existing_write_path_still_rejects_known_bug_to_bug():
    with pytest.raises(KGPrimitiveError) as exc_info:
        _validate_local_edge_pair(
            "originates_from",
            "Bug",
            "Bug",
            session_id="session-kgh1",
        )

    assert exc_info.value.code == "invalid_edge_endpoint_types"


@pytest.mark.asyncio
async def test_kgh1_mcp_use_case_delegates_without_commit(monkeypatch):
    captured = {}

    def _fake_audit(board_id, **kwargs):
        captured["board_id"] = board_id
        captured.update(kwargs)
        return {"board_id": board_id, "items": [], "mutated": False}

    monkeypatch.setattr(
        "okto_pulse.core.services.application_kg.audit_originates_from_contract",
        _fake_audit,
    )

    result = await AuditOriginatesFromContractUseCase().execute(
        AuditOriginatesFromContractCommand(
            "board-kgh1",
            limit=7,
            offset=3,
            include_ok=True,
        ),
        actor=ActorContext("agent-kgh1", "mcp", board_id="board-kgh1"),
        uow=object(),
    )

    assert result.data == {"board_id": "board-kgh1", "items": [], "mutated": False}
    assert captured == {
        "board_id": "board-kgh1",
        "limit": 7,
        "offset": 3,
        "include_ok": True,
    }
