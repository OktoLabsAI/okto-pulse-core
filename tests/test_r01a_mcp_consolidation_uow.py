"""Spec R01A MCP-FU1 — KG consolidation write tools on the UnitOfWork path.

The three consolidation write tools (begin/propose/commit_consolidation) now route
through transport-free use cases + the MCP ``UnitOfWorkFactory`` instead of a raw
``get_db()`` session. Write parity is proven EMPIRICALLY (codex: prove, don't
assume):

- golden success: a full begin->add->propose->commit driven through the use cases
  PERSISTS the canonical node to the board graph — identical to the legacy
  primitive+db path (test_kg_cognitive_canonical_invariant). This proves the
  commit primitive's graph persistence runs on ``session_of(uow)``. The MCP
  adapter then commits the UnitOfWork once so relational records staged by the
  use case become durable in the same transaction.
- rollback oracle: a mid-commit failure persists NO partial node (no leak).
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from kg_registry_testing import configure_real_graph_test_kg_registry
from test_kg_cognitive_canonical_invariant import (
    _get_decision_layer_maturity,
    _seed_spec_root_and_decision,
)

from okto_pulse.core.application.use_cases import (
    BeginConsolidationCommand,
    BeginConsolidationUseCase,
    CommitConsolidationCommand,
    CommitConsolidationUseCase,
    ProposeReconciliationCommand,
    ProposeReconciliationUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    add_edge_candidate,
    add_node_candidate,
    finalize_deferred_consolidation,
)
from kg_schema_testing import bootstrap_board_graph
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    EdgeCandidate,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.source_maturity import MATURITY_CANONICAL_ELIGIBLE
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
def _uowf(db_factory):
    """The MCP UnitOfWorkFactory equivalent over the test session factory — the
    same shape get_unit_of_work_factory_for_mcp() builds in production."""
    return lambda: SQLAlchemyUnitOfWorkFactory(db_factory)


@pytest.fixture
def fu1_board():
    """A graph-isolated board for this suite — a unique board id with its own
    bootstrapped graph, so the consolidation commits here never bleed into the
    shared BOARD_ID graph used by other real-graph suites."""
    board = f"board-r01a-fu1-{uuid.uuid4().hex[:8]}"
    bootstrap_board_graph(board)
    return board


async def _begin_add_propose(board_id, agent_id, db_factory, spec_id, decision_ref, existing_decision_id):
    actor = ActorContext(agent_id, "mcp")
    uowf = _uowf(db_factory)

    async with uowf()(actor=actor) as uow:
        begin = (
            await BeginConsolidationUseCase().execute(
                BeginConsolidationCommand(
                    BeginConsolidationRequest(
                        board_id=board_id,
                        artifact_type="spec",
                        artifact_id=spec_id,
                        raw_content="r01a mcp-fu1 uow persist",
                    )
                ),
                actor=actor,
                uow=uow,
            )
        ).resp

    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="cog_canonical",
                node_type=KGNodeType.DECISION,
                title="Canonical decision",
                source_artifact_ref=decision_ref,
                maturity_status="working_immature",
            ),
        ),
        agent_id=agent_id,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="cog_canonical_depends",
                edge_type=KGEdgeType.DEPENDS_ON,
                from_candidate_id="cog_canonical",
                to_candidate_id=f"kg:{existing_decision_id}",
                confidence=0.8,
                layer="cognitive",
                rule_id="test/r01a_fu1",
            ),
        ),
        agent_id=agent_id,
    )

    async with uowf()(actor=actor) as uow:
        await ProposeReconciliationUseCase().execute(
            ProposeReconciliationCommand(
                ProposeReconciliationRequest(session_id=begin.session_id)
            ),
            actor=actor,
            uow=uow,
        )
    return begin


@pytest.mark.asyncio
async def test_consolidation_via_use_cases_persists_canonical(
    fu1_board, agent_id, db_factory
):
    """Golden write parity: begin->add->propose->commit through the UoW use cases
    PERSISTS the canonical node to the board graph (no commit(uow) needed)."""
    board_id = fu1_board
    configure_real_graph_test_kg_registry()
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    spec_ref = f"spec:{spec_id}"
    _root_id, existing_decision_id = _seed_spec_root_and_decision(board_id, spec_ref)
    decision_ref = f"{spec_ref}:decision:canon"

    begin = await _begin_add_propose(
        board_id, agent_id, db_factory, spec_id, decision_ref, existing_decision_id
    )

    actor = ActorContext(agent_id, "mcp")
    async with _uowf(db_factory)()(actor=actor) as uow:
        commit = (
            await CommitConsolidationUseCase().execute(
                CommitConsolidationCommand(
                    CommitConsolidationRequest(session_id=begin.session_id),
                    board_id=board_id,
                ),
                actor=actor,
                uow=uow,
            )
        ).resp
        await uow.commit()
        await finalize_deferred_consolidation(
            begin.session_id,
            agent_id=agent_id,
        )

    assert commit.connectivity["passed"] is True
    layer, maturity = _get_decision_layer_maturity(board_id, decision_ref)
    assert layer == "canonical"
    assert maturity == MATURITY_CANONICAL_ELIGIBLE


class _MCPRegistryDouble:
    """Minimal FastMCP double that captures registered tool callables."""

    def __init__(self) -> None:
        self.tools: dict[str, object] = {}

    def tool(self):
        def _decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return _decorator


class _RecordingUow:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.commits = 0
        self.rollbacks = 0
        self.fail_commit = False

    async def commit(self) -> None:
        self.events.append("commit")
        self.commits += 1
        if self.fail_commit:
            raise RuntimeError("relational commit failed")

    async def rollback(self) -> None:
        self.events.append("rollback")
        self.rollbacks += 1


class _RecordingUowContext:
    def __init__(self, uow: _RecordingUow) -> None:
        self.uow = uow

    async def __aenter__(self) -> _RecordingUow:
        self.uow.events.append("enter")
        return self.uow

    async def __aexit__(self, exc_type, _exc, _tb) -> None:
        if exc_type is not None:
            await self.uow.rollback()
        self.uow.events.append("exit")


class _RecordingUowFactory:
    def __init__(self, uow: _RecordingUow) -> None:
        self.uow = uow

    def __call__(self, *, actor):
        assert actor.actor_id == "agent-mcp-commit"
        return _RecordingUowContext(self.uow)


class _ToolResponse:
    def model_dump_json(self) -> str:
        return '{"committed":true}'


async def _registered_commit_tool(monkeypatch, execute):
    from okto_pulse.core.application import use_cases
    from okto_pulse.core.mcp import kg_tools

    events: list[str] = []
    uow = _RecordingUow(events)
    factory = _RecordingUowFactory(uow)
    registry = _MCPRegistryDouble()

    async def _get_agent():
        return SimpleNamespace(id="agent-mcp-commit")

    async def _require_session(
        _session_id: str,
        _agent_id: str,
        *,
        allow_pending_commit: bool = False,
    ):
        assert allow_pending_commit is True
        return SimpleNamespace(board_id="board-mcp-commit")

    class _CommitUseCaseDouble:
        async def execute(self, command, *, actor, uow):
            return await execute(command, actor=actor, uow=uow)

    monkeypatch.setattr(kg_tools, "_require_open_session", _require_session)

    async def _finalize(_session_id: str, *, agent_id: str) -> None:
        assert agent_id == "agent-mcp-commit"
        events.append("finalize")

    async def _abort(_session_id: str, *, agent_id: str) -> None:
        assert agent_id == "agent-mcp-commit"
        events.append("abort")

    monkeypatch.setattr(kg_tools, "finalize_deferred_consolidation", _finalize)
    monkeypatch.setattr(kg_tools, "abort_deferred_consolidation", _abort)
    monkeypatch.setattr(
        use_cases, "CommitConsolidationUseCase", _CommitUseCaseDouble
    )
    kg_tools.register_kg_tools(
        registry,
        get_agent=_get_agent,
        get_uow=lambda: factory,
    )
    return registry.tools["okto_pulse_kg_commit_consolidation"], uow, events


@pytest.mark.asyncio
async def test_mcp_commit_consolidation_commits_uow_once_after_execute(
    monkeypatch,
):
    async def _execute(_command, *, actor, uow):
        assert actor.actor_id == "agent-mcp-commit"
        assert uow.commits == 0
        uow.events.append("execute")
        return SimpleNamespace(resp=_ToolResponse())

    tool, uow, events = await _registered_commit_tool(monkeypatch, _execute)

    assert await tool(session_id="session-success") == '{"committed":true}'
    assert uow.commits == 1
    assert uow.rollbacks == 0
    assert events == ["enter", "execute", "commit", "finalize", "exit"]


@pytest.mark.asyncio
async def test_mcp_commit_consolidation_does_not_commit_and_rolls_back_on_failure(
    monkeypatch,
):
    async def _execute(_command, *, actor, uow):
        assert actor.actor_id == "agent-mcp-commit"
        uow.events.append("execute")
        raise KGPrimitiveError("commit_failed", "expected failure")

    tool, uow, events = await _registered_commit_tool(monkeypatch, _execute)

    response = await tool(session_id="session-failure")

    assert '"code": "commit_failed"' in response
    assert uow.commits == 0
    assert uow.rollbacks == 1
    assert "abort" not in events
    assert events == ["enter", "execute", "rollback", "exit"]


@pytest.mark.asyncio
async def test_mcp_relational_commit_failure_compensates_graph_before_returning(
    monkeypatch,
):
    async def _execute(_command, *, actor, uow):
        assert actor.actor_id == "agent-mcp-commit"
        uow.events.append("execute")
        return SimpleNamespace(resp=_ToolResponse())

    tool, uow, events = await _registered_commit_tool(monkeypatch, _execute)
    uow.fail_commit = True

    with pytest.raises(RuntimeError, match="relational commit failed"):
        await tool(session_id="session-relational-failure")

    assert uow.commits == 1
    assert uow.rollbacks == 1
    assert "finalize" not in events
    assert events == [
        "enter",
        "execute",
        "commit",
        "rollback",
        "exit",
        "abort",
    ]


@pytest.mark.asyncio
async def test_competing_mcp_commit_cannot_abort_owner_pending_snapshot(
    monkeypatch,
):
    async def _execute(_command, *, actor, uow):
        assert actor.actor_id == "agent-mcp-commit"
        uow.events.append("execute")
        raise KGPrimitiveError(
            "session_commit_in_progress",
            "owner commit is still in flight",
        )

    tool, uow, events = await _registered_commit_tool(monkeypatch, _execute)

    response = await tool(session_id="session-competing-commit")

    assert '"code": "session_commit_in_progress"' in response
    assert uow.commits == 0
    assert uow.rollbacks == 1
    assert "abort" not in events
    assert events == ["enter", "execute", "rollback", "exit"]


@pytest.mark.asyncio
async def test_mcp_cancel_during_execute_rolls_back_after_primitive_cleanup(
    monkeypatch,
):
    execute_entered = asyncio.Event()
    allow_cleanup = asyncio.Event()

    async def _execute(_command, *, actor, uow):
        assert actor.actor_id == "agent-mcp-commit"
        uow.events.append("execute")
        execute_entered.set()
        try:
            await allow_cleanup.wait()
        except asyncio.CancelledError:
            # Witness the primitive contract: its cancellation handler drains
            # and compensates graph state before UOW rollback regains control.
            uow.events.append("primitive_cleanup")
            raise

    tool, uow, events = await _registered_commit_tool(monkeypatch, _execute)
    parent = asyncio.create_task(tool(session_id="session-cancel-during-execute"))
    await execute_entered.wait()
    parent.cancel()
    allow_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await parent

    assert uow.commits == 0
    assert uow.rollbacks == 1
    assert "abort" not in events  # adapter never aborts an unowned snapshot
    assert events == [
        "enter",
        "execute",
        "primitive_cleanup",
        "rollback",
        "exit",
    ]


@pytest.mark.asyncio
async def test_consolidation_commit_failure_persists_no_partial_node(
    fu1_board, agent_id, db_factory
):
    """Rollback oracle: a failure in the middle of the commit (graph write raises)
    persists NO partial canonical node — no leak."""
    board_id = fu1_board
    configure_real_graph_test_kg_registry()
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    spec_ref = f"spec:{spec_id}"
    _root_id, existing_decision_id = _seed_spec_root_and_decision(board_id, spec_ref)
    decision_ref = f"{spec_ref}:decision:canon"

    begin = await _begin_add_propose(
        board_id, agent_id, db_factory, spec_id, decision_ref, existing_decision_id
    )

    actor = ActorContext(agent_id, "mcp")
    with patch(
        "okto_pulse.core.kg.primitives._apply_graph_node_create",
        side_effect=RuntimeError("mid-commit failure (R01A MCP-FU1)"),
    ):
        try:
            async with _uowf(db_factory)()(actor=actor) as uow:
                await CommitConsolidationUseCase().execute(
                    CommitConsolidationCommand(
                        CommitConsolidationRequest(session_id=begin.session_id),
                        board_id=board_id,
                    ),
                    actor=actor,
                    uow=uow,
                )
        except Exception:
            pass  # the commit failed mid-flow (raised or wrapped) — expected

    # No partial persistence: the canonical node never landed in the board graph.
    layer, _maturity = _get_decision_layer_maturity(board_id, decision_ref)
    assert layer != "canonical"


# --- structural strangler proofs -------------------------------------------


def _ast_fn(module, name):
    import ast
    from pathlib import Path

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node, tree
    raise AssertionError(f"{name} not found in {module.__file__}")


def test_migrated_consolidation_tools_have_no_relational_session():
    import ast

    from okto_pulse.core.mcp import kg_tools

    for tool in (
        "okto_pulse_kg_begin_consolidation",
        "okto_pulse_kg_propose_reconciliation",
        "okto_pulse_kg_commit_consolidation",
    ):
        node, _ = _ast_fn(kg_tools, tool)
        names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        assert "get_db" not in names, tool
        assert "get_db_for_mcp" not in names, tool
        assert "get_uow" in names, tool

    reg, _ = _ast_fn(kg_tools, "register_kg_tools")
    params = [a.arg for a in reg.args.kwonlyargs]
    assert "get_db" not in params and "get_uow" in params


def test_server_injects_uow_not_get_db_for_mcp_into_kg_tools():
    import ast
    from pathlib import Path

    from okto_pulse.core.mcp import server as mcp_server

    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_register_kg_tools"
    ]
    assert len(calls) == 1
    keywords = {keyword.arg: keyword.value for keyword in calls[0].keywords}
    assert isinstance(keywords.get("get_uow"), ast.Name)
    assert keywords["get_uow"].id == "get_unit_of_work_factory_for_mcp"
    assert "get_db" not in keywords


def test_commit_tool_preserves_ownership_precheck_and_r7_hold():
    """The migration kept the session ownership pre-check and the R7 cognitive-hold
    side effect in the commit adapter (only the session source changed)."""
    import ast

    from okto_pulse.core.mcp import kg_tools

    node, _ = _ast_fn(kg_tools, "okto_pulse_kg_commit_consolidation")
    called = {
        c.func.id if isinstance(c.func, ast.Name) else getattr(c.func, "attr", None)
        for c in ast.walk(node)
        if isinstance(c, ast.Call)
    }
    assert "_require_open_session" in called
    assert "_maybe_record_r7_cognitive_hold" in called
