from __future__ import annotations

import re
from pathlib import Path

import pytest

from okto_pulse.core.kg.primitives import KGPrimitiveError, _validate_local_edge_pair
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
    def __init__(self, *results: _FakeResult):
        self.results = list(results)
        self.statements: list[tuple[str, dict]] = []

    def execute(self, statement: str, params: dict):
        self.statements.append((statement, params))
        if self.results:
            return self.results.pop(0)
        return _FakeResult(has_row=False)


def test_create_edge_requires_materialized_relationship():
    exists_result = _FakeResult(has_row=False)
    create_result = _FakeResult(has_row=False)
    conn = _FakeConnection(exists_result, create_result)
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
    assert exists_result.closed is True
    assert create_result.closed is True
    assert "RETURN r.created_by_session_id LIMIT 1" in conn.statements[0][0]
    assert "RETURN r.created_by_session_id" in conn.statements[1][0]


def test_create_edge_counts_only_confirmed_relationship():
    exists_result = _FakeResult(has_row=False)
    create_result = _FakeResult(has_row=True)
    conn = _FakeConnection(exists_result, create_result)
    orch = TransactionOrchestrator(
        conn,
        sqlite_session=None,  # type: ignore[arg-type]
        session_id="sess-edge",
        board_id="board-edge",
    )

    orch.create_edge("supersedes", "decision-new", "decision-old")

    assert orch.counters.edges_added == 1
    assert len(orch.records) == 1
    assert exists_result.closed is True
    assert create_result.closed is True


def test_create_edge_skips_existing_relationship():
    exists_result = _FakeResult(has_row=True)
    conn = _FakeConnection(exists_result)
    orch = TransactionOrchestrator(
        conn,
        sqlite_session=None,  # type: ignore[arg-type]
        session_id="sess-edge",
        board_id="board-edge",
    )

    orch.create_edge("supersedes", "decision-new", "decision-old")

    assert orch.counters.edges_added == 0
    assert orch.records == []
    assert exists_result.closed is True
    assert len(conn.statements) == 1


def test_invalid_local_edge_pair_gets_contextual_error():
    with pytest.raises(KGPrimitiveError) as excinfo:
        _validate_local_edge_pair(
            "relates_to",
            "Entity",
            "Requirement",
            session_id="sess-edge",
        )

    assert excinfo.value.code == "invalid_edge_endpoint_types"
    assert "relates_to" in excinfo.value.message
    assert "Decision" in excinfo.value.message
    assert excinfo.value.details["allowed_pairs"] == [
        {"from_type": "Decision", "to_type": "Alternative"}
    ]


def test_agent_instructions_define_kg_consolidation_boundaries():
    instructions = (
        Path(__file__).parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "agent_instructions.md"
    ).read_text(encoding="utf-8")

    assert "ideations are discovery artifacts" in instructions
    assert "not consolidated directly into the KG" in instructions
    assert "`relates_to`" in instructions
    assert "Decision -> Alternative" in instructions
    assert "`implements`, `tests`, `belongs_to`, `mentions`, `violates`" in instructions
    assert "force_full_rebuild=true" in instructions


def test_agent_instructions_require_qna_for_ambiguity_and_artifacts():
    instructions = (
        Path(__file__).parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "agent_instructions.md"
    ).read_text(encoding="utf-8")

    assert "Ambiguity left unresolved at ideation is not free" in instructions
    assert "Every inferred requirement becomes latent rework" in instructions
    assert "more time and more tokens" in instructions
    assert "Use Q&A before creating or finalizing mockups" in instructions
    assert "Use Q&A before creating or finalizing architecture designs" in instructions
    assert "standard artifact for multi-layer systems" in instructions
    assert "regardless of overall complexity" in instructions
    assert "Creating mockups or Architecture Design before resolving" in instructions


def test_agent_instructions_contract_matches_current_mcp_surface():
    instructions = (
        Path(__file__).parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "agent_instructions.md"
    ).read_text(encoding="utf-8")

    assert "Kuzu" not in instructions
    assert "Kùzu" not in instructions
    assert "graph.kuzu" not in instructions
    assert "discovery.kuzu" not in instructions
    assert "post-Sprint" not in instructions
    assert "pattern correto" not in instructions
    assert "spec 3d907a87" not in instructions
    assert "spec d754d004" not in instructions
    assert "graph.lbug" in instructions
    assert "discovery.lbug" in instructions
    assert "okto_pulse_kg_begin_consolidation" in instructions
    assert "okto_pulse_kg_query_natural" in instructions
    assert "okto_pulse_get_analytics" in instructions
    assert "Sprint closes (moves to `closed`)" in instructions
    assert "Session/card pre-flight sequence" in instructions
    assert "MCP server does not prove that you read context" in instructions
    assert "interfaces do not own source/target" in instructions


def test_agent_instructions_do_not_use_bare_mcp_tool_aliases():
    repo_root = Path(__file__).parents[1]
    instructions = (
        repo_root / "src" / "okto_pulse" / "core" / "mcp" / "agent_instructions.md"
    ).read_text(encoding="utf-8")
    tool_names: set[str] = set()

    for path in (repo_root / "src" / "okto_pulse" / "core" / "mcp").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        tool_names.update(re.findall(r"^\s*async def (okto_pulse_[a-zA-Z0-9_]+)\(", source, re.MULTILINE))

    bare_aliases = []
    for name in sorted(tool_names):
        alias = name.removeprefix("okto_pulse_")
        if re.search(rf"(?<!okto_pulse_)\b{re.escape(alias)}\b", instructions):
            bare_aliases.append(alias)

    assert bare_aliases == []
